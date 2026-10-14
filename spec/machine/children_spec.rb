# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
require_relative '../support/machine_helpers'

module MachineChildrenTest
  def self.message(base, name, keys, *attrs)
    base.define("machine_children_test.#{name}") { (keys + attrs).each { |a| attribute a, String } }
  end

  ORDER = %i[order_id].freeze
  LINE = %i[order_id line_id].freeze
  PAYMENT = %i[order_id payment_id].freeze
  PARCEL = %i[order_id parcel_id].freeze

  def self.command(name, keys, *attrs) = message(Sourced::Command, name, keys, *attrs)
  def self.event(name, keys, *attrs) = message(Sourced::Event, name, keys, *attrs)

  # Order
  PlaceOrder = command(:place_order, ORDER, :currency, :total)
  OrderPlaced = event(:order_placed, ORDER, :currency, :total)
  AddLine = command(:add_line, LINE, :sku)
  CancelOrder = command(:cancel_order, ORDER)

  # Line items
  StockReserved = command(:stock_reserved, LINE)
  StockUnavailable = command(:stock_unavailable, LINE)
  PickLine = command(:pick_line, LINE)
  ReserveStock = command(:reserve_stock, LINE, :sku, :currency)
  ReleaseStock = command(:release_stock, LINE, :sku)

  # Payments
  RequestPayment = command(:request_payment, ORDER)
  TimeOutPayment = command(:time_out_payment, ORDER)
  GatewayCaptured = command(:gateway_captured, PAYMENT)
  GatewayDeclined = command(:gateway_declined, PAYMENT)
  ChargeCard = command(:charge_card, PAYMENT, :amount)

  # Parcels
  ShipParcel = command(:ship_parcel, PARCEL)
  PackParcel = command(:pack_parcel, PARCEL)
  PrintLabel = command(:print_label, PARCEL)

  LineState = Struct.new(:sku, :currency)

  # What parents' after_sync hooks saw: [claimed message class, event classes]
  SYNCED = []

  class LineItemMachine < Sourced::Machine
    partition_by :order_id, :line_id
    Cancelled = Checkpoint(:cancelled)
    on_stop Cancelled

    Added = Checkpoint(:added)
    Reserved = Checkpoint(:reserved)
    Picked = Checkpoint(:picked)
    Failed = Checkpoint(:failed)

    context { LineState.new }
    evolve(Started) { |line, evt| line.sku = evt.payload.input.transform_keys(&:to_sym).fetch(:sku) }
    # The parent's fact: never wakes a line, but is in its partition reads
    evolve(OrderPlaced) { |line, evt| line.currency = evt.payload.currency }

    start Added do
      entry { |line, _evt| dispatch ReserveStock, sku: line.sku, currency: line.currency }
      command StockReserved => Reserved
      command StockUnavailable => Failed
    end

    checkpoint Reserved do
      exit { |line, _evt| dispatch ReleaseStock, sku: line.sku }
      command PickLine => Picked
    end

    final Picked
    final Failed
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)
    LinesDone = Checkpoint(:lines_done)
    LinesPartiallyFailed = Checkpoint(:lines_partially_failed)
    Cancelled = Checkpoint(:cancelled)

    spawns LineItemMachine, as: :lines, stop_on: Cancelled

    initial do
      command PlaceOrder => Placed do |_order, cmd|
        event OrderPlaced, cmd.payload.to_h
      end
    end

    checkpoint Placed do
      command(AddLine) { |_order, cmd| spawn :lines, line_id: cmd.payload.line_id, sku: cmd.payload.sku }

      children_done lines: LinesDone | LinesPartiallyFailed do |_order|
        all_picked = machine.children(:lines).all? { |line| line.state == LineItemMachine::Picked }
        to(all_picked ? LinesDone : LinesPartiallyFailed)
      end

      command CancelOrder => Cancelled
    end

    final LinesDone
    final LinesPartiallyFailed
    final Cancelled

    after_sync { |messages:, events:, **| SYNCED << [messages.first.class, events.map(&:class)] }
  end

  PaymentState = Struct.new(:amount)

  class PaymentMachine < Sourced::Machine
    partition_by :order_id, :payment_id
    Voided = Checkpoint(:voided)
    on_stop Voided

    Requested = Checkpoint(:requested)
    Captured = Checkpoint(:captured)
    Declined = Checkpoint(:declined)

    context { PaymentState.new }
    evolve(OrderPlaced) { |pay, evt| pay.amount = evt.payload.total }

    start Requested do
      entry { |pay, _evt| dispatch ChargeCard, amount: pay.amount }
      command GatewayCaptured => Captured
      command GatewayDeclined => Declined
    end

    final Captured
    final Declined
  end

  class BillingMachine < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)
    AwaitingPayment = Checkpoint(:awaiting_payment)
    Paid = Checkpoint(:paid)
    PaymentFailed = Checkpoint(:payment_failed)
    TimedOut = Checkpoint(:timed_out)

    initial do
      command PlaceOrder => Placed do |_order, cmd|
        event OrderPlaced, cmd.payload.to_h
      end
    end

    checkpoint Placed do
      command RequestPayment => AwaitingPayment
    end

    checkpoint AwaitingPayment do
      invoke PaymentMachine, as: :payment,
                             keys: ->(_order) { { payment_id: SecureRandom.uuid } },
                             on_done: { PaymentMachine::Captured => Paid, PaymentMachine::Declined => PaymentFailed }
      command TimeOutPayment => TimedOut
    end

    checkpoint PaymentFailed do
      command RequestPayment => AwaitingPayment
    end

    checkpoint TimedOut
    final Paid

    after_sync { |messages:, events:, **| SYNCED << [messages.first.class, events.map(&:class)] }
  end

  # A parcel starts in a compound checkpoint: Preparing, entered at Packing.
  class PackingMachine < Sourced::Machine
    Packing = Checkpoint(:packing)
    Packed = Checkpoint(:packed)

    start Packing do
      entry { |_parcel, _evt| dispatch PrintLabel }
      command PackParcel => Packed
    end

    final Packed
  end

  class ParcelMachine < Sourced::Machine
    partition_by :order_id, :parcel_id
    on_stop :cancelled

    Preparing = Checkpoint(:preparing)
    Shipped = Checkpoint(:shipped)

    start Preparing, PackingMachine, on_done: { PackingMachine::Packed => Shipped }
    final Shipped
  end

  class ShippingMachine < Sourced::Machine
    partition_by :order_id

    Shipped = Checkpoint(:shipped)

    spawns ParcelMachine, as: :parcels

    initial do
      command(ShipParcel) { |_order, cmd| spawn :parcels, parcel_id: cmd.payload.parcel_id }
      children_done parcels: Shipped
    end

    final Shipped
  end

  RSpec.describe 'Sourced::Machine children' do
    include Sourced::Testing::RSpec
    include MachineHelpers

    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }

    def run(router, *messages)
      store.append(messages)
      router.drain
    end

    def log = store.read_all(limit: 200).messages
    def transitions(machine) = log.grep(machine::Transitioned)

    def moves(machine) = transitions(machine).map do |m|
      m.payload.to_h.slice(:trigger, :source, :target, :exited, :entered)
    end

    before { SYNCED.clear }

    describe 'Stop, Started and Finished' do
      let(:keys) { { order_id: 'o1', line_id: 'l1' } }

      it 'are generated per machine, keyed by its partition keys' do
        expect(LineItemMachine::Stop.type).to eq('machine_children_test.line_item_machine.stop')
        expect(LineItemMachine::Stop.payload_attribute_names).to eq(%i[order_id line_id])
        expect(LineItemMachine.handled_commands).to include(LineItemMachine::Stop)

        expect(LineItemMachine::Started.type).to eq('machine_children_test.line_item_machine.started')
        expect(LineItemMachine::Started.payload_attribute_names).to include(:order_id, :line_id, :input, :entered)
        expect(LineItemMachine::Finished.type).to eq('machine_children_test.line_item_machine.finished')
        expect(LineItemMachine::Finished.payload_attribute_names).to eq(%i[order_id line_id final])
      end

      it 'accepts Stop once started, until final, and exits as usual' do
        stop = LineItemMachine::Stop.new(payload: keys)
        line = LineItemMachine.new(keys)
        expect(line.handles_command?(stop)).to be(false)

        line.evolve([LineItemMachine::Started.new(payload: {
          **keys, input: { sku: 'A1' }, trigger: 'start', source: 'initial', target: 'added', exited: [], entered: ['added']
                                                  })])
        expect(line.handles_command?(stop)).to be(true)

        transitioned, finished, *rest = line.decide(stop)
        expect(rest).to be_empty
        expect(transitioned.payload).to eq(moved(LineItemMachine, 'stop', from: 'added', to: 'cancelled',
                                                                          exited: ['added'], **keys).payload)
        expect(finished.payload.to_h).to eq(**keys, final: 'cancelled')
        expect(line.handles_command?(stop)).to be(false)
      end
    end

    describe 'spawning' do
      let(:router) { Sourced::Router.new(store:, reactors: [OrderMachine, LineItemMachine]).setup! }

      def place_and_add_lines
        run(router, PlaceOrder.new(payload: { order_id: 'o1', currency: 'EUR', total: '100' }))
        run(router,
            AddLine.new(payload: { order_id: 'o1', line_id: 'l1', sku: 'A1' }),
            AddLine.new(payload: { order_id: 'o1', line_id: 'l2', sku: 'B2' }))
      end

      def line(command, line_id) = command.new(payload: { order_id: 'o1', line_id: })
      def order = OrderMachine.load(store:, order_id: 'o1')
      def lines = order.children[:lines].to_h { |ref| [ref.keys[:line_id], ref.state] }

      it 'spawns children as data, tracked by the parent' do
        with_reactor(OrderMachine, order_id: 'o1')
          .given(moved(OrderMachine, 'machine_children_test.place_order', from: 'initial', to: 'placed',
                                                                          order_id: 'o1'))
          .when(AddLine, order_id: 'o1', line_id: 'l1', sku: 'A1')
          .then(LineItemMachine::Started.new(payload: {
                                               order_id: 'o1', line_id: 'l1', input: { sku: 'A1' },
                                               trigger: 'start', source: 'initial', target: 'added', exited: [], entered: ['added']
                                             }))
      end

      it 'runs each child in its own partition, evolving the parent facts it needs' do
        place_and_add_lines

        reserves = log.grep(ReserveStock)
        expect(reserves.map { |m| [m.payload.line_id, m.payload.sku, m.payload.currency] })
          .to contain_exactly(%w[l1 A1 EUR], %w[l2 B2 EUR])
        expect(lines).to eq('l1' => LineItemMachine::Added, 'l2' => LineItemMachine::Added)
      end

      it 'joins when every child is final' do
        place_and_add_lines
        run(router, line(StockReserved, 'l1'), line(StockReserved, 'l2'), line(PickLine, 'l1'))
        expect(order.value).to eq(OrderMachine::Placed)

        run(router, line(PickLine, 'l2'))
        expect(order.value).to eq(OrderMachine::LinesDone)
        expect(lines).to eq('l1' => LineItemMachine::Picked, 'l2' => LineItemMachine::Picked)
        expect(moves(OrderMachine).last)
          .to eq(trigger: 'children_done :lines', source: 'placed', target: 'lines_done', exited: ['placed'],
                 entered: ['lines_done'])
        expect(log.grep(LineItemMachine::Finished).map { |m| [m.payload.line_id, m.payload.final] })
          .to contain_exactly(%w[l1 picked], %w[l2 picked])
      end

      it "hands a join completed by a child's finish to after_sync" do
        place_and_add_lines
        run(router, line(StockReserved, 'l1'), line(StockReserved, 'l2'), line(PickLine, 'l1'), line(PickLine, 'l2'))

        expect(SYNCED).to include([LineItemMachine::Finished, [OrderMachine::Transitioned]])
        expect(SYNCED).to include([AddLine, [LineItemMachine::Started]])
      end

      it 'decides the join from the children it ended with' do
        place_and_add_lines
        run(router, line(StockReserved, 'l1'), line(PickLine, 'l1'), line(StockUnavailable, 'l2'))

        expect(order.value).to eq(OrderMachine::LinesPartiallyFailed)
      end

      it 'sends Stop to the live children when it reaches stop_on' do
        place_and_add_lines
        run(router, line(StockReserved, 'l1'), line(StockUnavailable, 'l2'))
        run(router, CancelOrder.new(payload: { order_id: 'o1' }))

        expect(log.grep(LineItemMachine::Stop).map { |m| m.payload.line_id }).to eq(['l1'])
        expect(lines).to eq('l1' => LineItemMachine::Cancelled, 'l2' => LineItemMachine::Failed)
        expect(log.grep(ReleaseStock).map { |m| m.payload.line_id }).to eq(['l1'])
        expect(order.value).to eq(OrderMachine::Cancelled)
      end

      it 'never wakes a child for a parent fact' do
        place_and_add_lines
        claims = []
        allow(LineItemMachine).to receive(:handle_claim).and_wrap_original do |original, claim, **kw|
          claims.concat(claim.messages.map(&:class))
          original.call(claim, **kw)
        end

        run(router, CancelOrder.new(payload: { order_id: 'o1' }))
        expect(claims).to eq([LineItemMachine::Stop, LineItemMachine::Stop,
                              LineItemMachine::Transitioned, LineItemMachine::Transitioned])
      end
    end

    describe 'invoking' do
      let(:router) { Sourced::Router.new(store:, reactors: [BillingMachine, PaymentMachine]).setup! }

      def billing = BillingMachine.load(store:, order_id: 'o1')
      def payment_ids = billing.children[:payment].map { |ref| ref.keys[:payment_id] }
      def gateway(command, payment_id) = command.new(payload: { order_id: 'o1', payment_id: })

      before do
        run(router, PlaceOrder.new(payload: { order_id: 'o1', currency: 'EUR', total: '100' }))
        run(router, RequestPayment.new(payload: { order_id: 'o1' }))
      end

      it 'spawns the child in the decision that enters the checkpoint' do
        awaiting = transitions(BillingMachine).find { |m| m.payload.target == 'awaiting_payment' }
        started, *others = log.grep(PaymentMachine::Started)
        expect(others).to be_empty
        expect(started.causation_id).to eq(awaiting.causation_id)
        expect(started.payload.to_h).to include(trigger: 'start', target: 'requested', entered: ['requested'])
        expect(payment_ids.size).to eq(1)
        expect(log.grep(ChargeCard).map { |m| m.payload.amount }).to eq(['100'])
      end

      it "maps the child's final checkpoint onto ours" do
        run(router, gateway(GatewayCaptured, payment_ids.first))

        expect(billing.value).to eq(BillingMachine::Paid)
        expect(billing.children[:payment].map(&:state)).to eq([PaymentMachine::Captured])
        expect(moves(BillingMachine).last)
          .to eq(trigger: PaymentMachine::Finished.type, source: 'awaiting_payment', target: 'paid', exited: ['awaiting_payment'],
                 entered: ['paid'])
      end

      it 'hands on_done mappings and invoked children to after_sync' do
        run(router, gateway(GatewayCaptured, payment_ids.first))

        expect(SYNCED).to include([RequestPayment, [BillingMachine::Transitioned, PaymentMachine::Started]])
        expect(SYNCED).to include([PaymentMachine::Finished, [BillingMachine::Transitioned]])
        expect(SYNCED.select { |source, _| source == PaymentMachine::Started }).to eq([[PaymentMachine::Started, []]])
      end

      it 'stops the child when the checkpoint is left, and ignores its late results' do
        payment_id = payment_ids.first
        run(router, TimeOutPayment.new(payload: { order_id: 'o1' }))

        expect(billing.value).to eq(BillingMachine::TimedOut)
        expect(billing.children[:payment].map(&:state)).to eq([PaymentMachine::Voided])

        run(router, gateway(GatewayCaptured, payment_id))
        expect(moves(PaymentMachine).map { |m| m[:target] }).to eq(['voided'])
        expect(billing.value).to eq(BillingMachine::TimedOut)
      end

      it 'invokes a fresh child on re-entry, and only maps the current one' do
        first = payment_ids.first
        run(router, gateway(GatewayDeclined, first))
        expect(billing.value).to eq(BillingMachine::PaymentFailed)

        run(router, RequestPayment.new(payload: { order_id: 'o1' }))
        expect(billing.value).to eq(BillingMachine::AwaitingPayment)
        expect(payment_ids.size).to eq(2)
        expect(payment_ids.last).not_to eq(first)

        run(router, gateway(GatewayCaptured, payment_ids.last))
        expect(billing.value).to eq(BillingMachine::Paid)
      end
    end

    describe 'a child starting in a compound checkpoint' do
      let(:router) { Sourced::Router.new(store:, reactors: [ShippingMachine, ParcelMachine]).setup! }
      let(:keys) { { order_id: 'o1', parcel_id: 'p1' } }

      def parcel = ParcelMachine.load(store:, **keys)
      def shipping = ShippingMachine.load(store:, order_id: 'o1')

      def started(**keys)
        ParcelMachine::Started.new(payload: {
          **keys, input: {}, trigger: 'start', source: 'initial', target: 'preparing',
                                     exited: [], entered: %w[preparing preparing.packing]
                                   })
      end

      it 'is started into its start and the nested start below it' do
        with_reactor(ShippingMachine, order_id: 'o1')
          .when(ShipParcel, **keys)
          .then(started(**keys))
      end

      it 'runs the nested entry actions, and finishes through on_done' do
        run(router, ShipParcel.new(payload: keys))

        expect(parcel.value).to eq(ParcelMachine::Preparing => PackingMachine::Packing)
        expect(log.grep(PrintLabel).map { |m| m.payload.to_h }).to eq([keys])
        expect(shipping.children[:parcels].map(&:state)).to eq([ParcelMachine::Preparing])

        run(router, PackParcel.new(payload: keys))

        expect(parcel.value).to eq(ParcelMachine::Shipped)
        expect(moves(ParcelMachine)).to eq([
                                             { trigger: 'machine_children_test.pack_parcel', source: 'preparing.packing', target: 'preparing.packed',
                                               exited: ['preparing.packing'], entered: ['preparing.packed'] },
                                             { trigger: 'on_done', source: 'preparing.packed', target: 'shipped',
                                               exited: %w[preparing.packed preparing], entered: ['shipped'] }
                                           ])
        expect(shipping.children[:parcels].map(&:state)).to eq([ParcelMachine::Shipped])
        expect(shipping.value).to eq(ShippingMachine::Shipped)
      end

      it 'is stopped from inside the nested start, exiting both levels' do
        parcel = ParcelMachine.new(keys)
        parcel.evolve([started(**keys)])

        transitioned, finished = parcel.decide(ParcelMachine::Stop.new(payload: keys))
        expect(transitioned.payload.to_h.slice(:trigger, :source, :target, :exited, :entered)).to eq(
          trigger: 'stop', source: 'preparing', target: 'cancelled',
          exited: %w[preparing.packing preparing], entered: ['cancelled']
        )
        expect(finished.payload.final).to eq('cancelled')
      end
    end

    describe 'definition checks' do
      def machine(*keys, &block)
        klass = Class.new(Sourced::Machine)
        stub_const("MachineChildrenTest::Spec#{klass.object_id}", klass)
        klass.partition_by(*(keys.empty? ? %i[order_id] : keys))
        klass.class_eval(&block)
        klass
      end

      it 'requires children to partition by the parent keys too' do
        other = Class.new(Sourced::Machine) do
          def self.name = 'MachineChildrenTest::Unrelated'
          partition_by :line_id
          start :added
          on_stop :cancelled
        end

        expect { machine { spawns other, as: :others }.validate! }
          .to raise_error(Sourced::Machine::DefinitionError, /must partition by our keys too, missing order_id/)
      end

      it 'validates its children' do
        child = machine(:order_id, :line_id) do
          start :added
          on_stop :cancelled
          Checkpoint(:stray)
        end
        parent = machine { spawns child, as: :lines }

        expect { parent.validate! }.to raise_error(Sourced::Machine::DefinitionError, /stray.* declared but never placed/)
      end

      it 'validates a child that spawns its parent back' do
        parent = machine do
          start :opened
          on_stop :closed
        end
        child = machine do
          start :added
          on_stop :cancelled
        end
        parent.spawns child, as: :children
        child.spawns parent, as: :parents

        expect(parent.validate!).to be(true)
        expect(child.tree).to equal(child.tree)
      end

      it 'requires children to declare start and on_stop' do
        no_stop = Class.new(Sourced::Machine) do
          partition_by :order_id, :line_id
          start :added
        end

        expect do
          machine do
            spawns no_stop, as: :x
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /has no on_stop/)
      end

      it 'checks stop_on, on_done and children_done' do
        expect { machine { spawns LineItemMachine, as: :lines, stop_on: LineItemMachine::Cancelled } }
          .to raise_error(Sourced::Machine::DefinitionError, /LineItemMachine's checkpoint, not/)

        bad_done = machine do
          checkpoint(:awaiting_payment) do
            invoke PaymentMachine, as: :payment, keys: ->(_) { {} }, on_done: { PaymentMachine::Requested => :paid }
          end
          final :paid
        end
        expect do
          bad_done.validate!
        end.to raise_error(Sourced::Machine::DefinitionError, /on_done .*requested isn't a final checkpoint/)

        expect do
          machine do
            checkpoint(:placed) { children_done(lines: :lines_done) }
            final :lines_done
          end.validate!
        end
          .to raise_error(Sourced::Machine::DefinitionError, /children_done :lines doesn't name spawned children/)
      end

      it 'requires on_stop machines to be named, after partition_by' do
        expect do
          Class.new(Sourced::Machine) do
            partition_by :order_id
            on_stop :cancelled
          end
        end
          .to raise_error(Sourced::Machine::DefinitionError, /needs a name/)
      end

      it 'requires reactions to use types with all partition keys' do
        m = machine(:order_id, :line_id) { reaction(OrderPlaced) { |_s, _e| nil } }
        expect do
          m.validate!
        end.to raise_error(Sourced::Machine::DefinitionError, /reaction .*OrderPlaced must declare line_id/)
      end
    end
  end
end
