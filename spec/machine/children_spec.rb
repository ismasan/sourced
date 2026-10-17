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

  # Declared starts
  StartLines = command(:start_lines, ORDER, :line_ids)
  LineRequested = event(:line_requested, LINE, :sku)

  LineState = Struct.new(:sku, :currency)

  # What parents' after_sync hooks saw: [claimed message class, event classes]
  SYNCED = []
  PAYLOAD_CALLS = []

  class LineItemMachine < Sourced::Machine
    partition_by :order_id, :line_id
    Cancelled = State(:cancelled)
    stopped Cancelled

    Added = State(:added)
    Reserved = State(:reserved)
    Picked = State(:picked)
    Failed = State(:failed)

    context { LineState.new }
    evolve(Started) { |line, evt| line.sku = evt.payload.input.transform_keys(&:to_sym).fetch(:sku) }
    # The parent's fact: never wakes a line, but is in its partition reads
    evolve(OrderPlaced) { |line, evt| line.currency = evt.payload.currency }

    started Added do
      entry { |line, _evt| dispatch ReserveStock, sku: line.sku, currency: line.currency }
      command StockReserved => Reserved
      command StockUnavailable => Failed
    end

    state Reserved do
      exit { |line, _evt| dispatch ReleaseStock, sku: line.sku }
      command PickLine => Picked
    end

    final Picked
    final Failed
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)
    LinesDone = State(:lines_done)
    LinesPartiallyFailed = State(:lines_partially_failed)
    Cancelled = State(:cancelled)

    initial do
      command PlaceOrder => Placed do |_order, cmd|
        event OrderPlaced, cmd.payload.to_h
      end
    end

    state Placed do
      command AddLine, starts: LineItemMachine

      children_done LineItemMachine => LinesDone | LinesPartiallyFailed do |_order|
        all_picked = machine.children(LineItemMachine).all? { |line| line.state == LineItemMachine::Picked }
        to(all_picked ? LinesDone : LinesPartiallyFailed)
      end

      command CancelOrder => Cancelled
    end

    final LinesDone
    final LinesPartiallyFailed
    final(Cancelled) { stops LineItemMachine }

    after_sync { |messages:, events:, **| SYNCED << [messages.first.class, events.map(&:class)] }
  end

  PaymentState = Struct.new(:amount)

  class PaymentMachine < Sourced::Machine
    partition_by :order_id, :payment_id
    Voided = State(:voided)
    stopped Voided

    Requested = State(:requested)
    Captured = State(:captured)
    Declined = State(:declined)

    context { PaymentState.new }
    evolve(OrderPlaced) { |pay, evt| pay.amount = evt.payload.total }

    started Requested do
      entry { |pay, _evt| dispatch ChargeCard, amount: pay.amount }
      command GatewayCaptured => Captured
      command GatewayDeclined => Declined
    end

    final Captured
    final Declined
  end

  class BillingMachine < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)
    AwaitingPayment = State(:awaiting_payment)
    Paid = State(:paid)
    PaymentFailed = State(:payment_failed)
    TimedOut = State(:timed_out)

    context { PaymentState.new }
    evolve(OrderPlaced) { |order, evt| order.amount = evt.payload.total }

    initial do
      command PlaceOrder => Placed do |_order, cmd|
        event OrderPlaced, cmd.payload.to_h
      end
    end

    state Placed do
      command RequestPayment => AwaitingPayment
    end

    state AwaitingPayment do
      start PaymentMachine do
        payload do |order, state|
          PAYLOAD_CALLS << [order.amount, state]
          { payment_id: SecureRandom.uuid, amount: order.amount, entered_by: state.id }
        end
        on_done PaymentMachine::Captured => Paid
        on_done PaymentMachine::Declined => PaymentFailed
      end
      command TimeOutPayment => TimedOut
    end

    state PaymentFailed do
      command RequestPayment => AwaitingPayment
    end

    state TimedOut
    final Paid

    after_sync { |messages:, events:, **| SYNCED << [messages.first.class, events.map(&:class)] }
  end

  # A parcel starts in a compound state: Preparing, entered at Packing.
  class PackingMachine < Sourced::Machine
    Packing = State(:packing)
    Packed = State(:packed)

    started Packing do
      entry { |_parcel, _evt| dispatch PrintLabel }
      command PackParcel => Packed
    end

    final Packed
  end

  class ParcelMachine < Sourced::Machine
    partition_by :order_id, :parcel_id
    stopped :cancelled

    Preparing = State(:preparing)
    Shipped = State(:shipped)

    started Preparing, PackingMachine, on_done: { PackingMachine::Packed => Shipped }
    final Shipped
  end

  class ShippingMachine < Sourced::Machine
    partition_by :order_id

    Shipped = State(:shipped)

    initial do
      command ShipParcel, starts: ParcelMachine
      children_done ParcelMachine => Shipped
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
    def transitions(machine) = log.select { |m| Sourced::Machine::State === m && m.class.owner.equal?(machine) }

    def moves(machine) = transitions(machine).map do |m|
      m.payload.to_h.slice(:trigger, :source, :target, :exited, :entered)
    end

    before { SYNCED.clear }

    describe 'Stop, Started and Finished' do
      let(:keys) { { order_id: 'o1', line_id: 'l1' } }

      it 'are generated per machine, keyed by its partition keys' do
        expect(LineItemMachine::Stop.type).to eq('machine_children_test.line_item_machine.runtime.child.stop')
        expect(LineItemMachine::Stop.payload_attribute_names).to eq(%i[order_id line_id])
        expect(LineItemMachine.handled_commands).to include(LineItemMachine::Stop)

        expect(LineItemMachine::Started.type).to eq('machine_children_test.line_item_machine.runtime.child.started')
        expect(LineItemMachine::Started.payload_attribute_names).to include(:order_id, :line_id, :input, :entered)
        expect(LineItemMachine::Finished.type).to eq('machine_children_test.line_item_machine.runtime.child.finished')
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

    describe 'starting from handlers' do
      let(:router) { Sourced::Router.new(store:, reactors: [OrderMachine, LineItemMachine]).setup! }

      def place_and_add_lines
        run(router, PlaceOrder.new(payload: { order_id: 'o1', currency: 'EUR', total: '100' }))
        run(router,
            AddLine.new(payload: { order_id: 'o1', line_id: 'l1', sku: 'A1' }),
            AddLine.new(payload: { order_id: 'o1', line_id: 'l2', sku: 'B2' }))
      end

      def line(command, line_id) = command.new(payload: { order_id: 'o1', line_id: })
      def order = OrderMachine.load(store:, order_id: 'o1')
      def lines = order.children[LineItemMachine].to_h { |ref| [ref.keys[:line_id], ref.state] }

      it 'starts children as data, tracked by the parent' do
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
          .to eq(trigger: 'children_done MachineChildrenTest::LineItemMachine', source: 'placed', target: 'lines_done', exited: ['placed'],
                 entered: ['lines_done'])
        expect(log.grep(LineItemMachine::Finished).map { |m| [m.payload.line_id, m.payload.final] })
          .to contain_exactly(%w[l1 picked], %w[l2 picked])
      end

      it "hands a join completed by a child's finish to after_sync" do
        place_and_add_lines
        run(router, line(StockReserved, 'l1'), line(StockReserved, 'l2'), line(PickLine, 'l1'), line(PickLine, 'l2'))

        expect(SYNCED).to include([LineItemMachine::Finished, [OrderMachine::LinesDone]])
        expect(SYNCED).to include([AddLine, [LineItemMachine::Started]])
      end

      it 'decides the join from the children it ended with' do
        place_and_add_lines
        run(router, line(StockReserved, 'l1'), line(PickLine, 'l1'), line(StockUnavailable, 'l2'))

        expect(order.value).to eq(OrderMachine::LinesPartiallyFailed)
      end

      it 'sends Stop to the live children when it enters a state that stops them' do
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
                              LineItemMachine::Cancelled, LineItemMachine::Cancelled])
      end
    end

    describe 'invoking' do
      let(:router) { Sourced::Router.new(store:, reactors: [BillingMachine, PaymentMachine]).setup! }

      def billing = BillingMachine.load(store:, order_id: 'o1')
      def payment_ids = billing.children[PaymentMachine].map { |ref| ref.keys[:payment_id] }
      def gateway(command, payment_id) = command.new(payload: { order_id: 'o1', payment_id: })

      before do
        PAYLOAD_CALLS.clear
        run(router, PlaceOrder.new(payload: { order_id: 'o1', currency: 'EUR', total: '100' }))
        run(router, RequestPayment.new(payload: { order_id: 'o1' }))
      end

      it 'builds the payload from the context and the event that entered the state' do
        awaiting = transitions(BillingMachine).find { |m| m.payload.target == 'awaiting_payment' }
        (amount, state), *others = PAYLOAD_CALLS
        expect(others).to be_empty
        expect(amount).to eq('100')
        expect(state).to be_instance_of(BillingMachine::AwaitingPayment)
        expect(state.id).to eq(awaiting.id)

        started = log.grep(PaymentMachine::Started).first
        expect(started.payload.input.transform_keys(&:to_sym)).to eq(amount: '100', entered_by: awaiting.id)
      end

      it 'records a non-deterministic payload once, in Started' do
        payment_id = log.grep(PaymentMachine::Started).first.payload.payment_id
        run(router, TimeOutPayment.new(payload: { order_id: 'o1' }))

        expect(PAYLOAD_CALLS.size).to eq(1)
        expect(payment_ids).to eq([payment_id])
        expect(log.grep(PaymentMachine::Started).size).to eq(1)
      end

      it 'starts the child in the decision that enters the state' do
        awaiting = transitions(BillingMachine).find { |m| m.payload.target == 'awaiting_payment' }
        started, *others = log.grep(PaymentMachine::Started)
        expect(others).to be_empty
        expect(started.causation_id).to eq(awaiting.causation_id)
        expect(started.payload.to_h).to include(trigger: 'start', target: 'requested', entered: ['requested'])
        expect(payment_ids.size).to eq(1)
        expect(log.grep(ChargeCard).map { |m| m.payload.amount }).to eq(['100'])
      end

      it "maps the child's final state onto ours" do
        run(router, gateway(GatewayCaptured, payment_ids.first))

        expect(billing.value).to eq(BillingMachine::Paid)
        expect(billing.children[PaymentMachine].map(&:state)).to eq([PaymentMachine::Captured])
        expect(moves(BillingMachine).last)
          .to eq(trigger: PaymentMachine::Finished.type, source: 'awaiting_payment', target: 'paid', exited: ['awaiting_payment'],
                 entered: ['paid'])
      end

      it "hands on_done mappings and states' children to after_sync" do
        run(router, gateway(GatewayCaptured, payment_ids.first))

        expect(SYNCED).to include([RequestPayment, [BillingMachine::AwaitingPayment, PaymentMachine::Started]])
        expect(SYNCED).to include([PaymentMachine::Finished, [BillingMachine::Paid]])
        expect(SYNCED.select { |source, _| source == PaymentMachine::Started }).to eq([[PaymentMachine::Started, []]])
      end

      it 'stops the child when the state is left, and ignores its late results' do
        payment_id = payment_ids.first
        run(router, TimeOutPayment.new(payload: { order_id: 'o1' }))

        expect(billing.value).to eq(BillingMachine::TimedOut)
        expect(billing.children[PaymentMachine].map(&:state)).to eq([PaymentMachine::Voided])

        run(router, gateway(GatewayCaptured, payment_id))
        expect(moves(PaymentMachine).map { |m| m[:target] }).to eq(['voided'])
        expect(billing.value).to eq(BillingMachine::TimedOut)
      end

      it 'starts a fresh child on re-entry, and only maps the current one' do
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

    describe 'a child starting in a compound state' do
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
        expect(shipping.children[ParcelMachine].map(&:state)).to eq([ParcelMachine::Preparing])

        run(router, PackParcel.new(payload: keys))

        expect(parcel.value).to eq(ParcelMachine::Shipped)
        expect(moves(ParcelMachine)).to eq([
                                             { trigger: 'machine_children_test.pack_parcel', source: 'preparing.packing', target: 'preparing.packed',
                                               exited: ['preparing.packing'], entered: ['preparing.packed'] },
                                             { trigger: 'on_done', source: 'preparing.packed', target: 'shipped',
                                               exited: %w[preparing.packed preparing], entered: ['shipped'] }
                                           ])
        expect(shipping.children[ParcelMachine].map(&:state)).to eq([ParcelMachine::Shipped])
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

    describe 'declared starts' do
      def machine(*keys, &block)
        klass = Class.new(Sourced::Machine)
        stub_const("MachineChildrenTest::Starts#{klass.object_id}", klass)
        klass.partition_by(*(keys.empty? ? %i[order_id] : keys))
        klass.class_eval(&block)
        klass
      end

      # A child keyed by the parent's keys alone.
      let(:audit) do
        machine do
          started :auditing
          stopped :dropped
        end
      end

      def lines(*ids) = StartLines.new(payload: { order_id: 'o1', line_ids: ids.join(',') })
      def started_lines(events) = events.grep(LineItemMachine::Started).map { |evt| evt.payload.line_id }

      def parent_with(starts)
        machine do
          initial do
            command(StartLines => :placed, starts:) do |_order, cmd|
              cmd.payload.line_ids.split(',').each { |id| start LineItemMachine, line_id: id, sku: id }
            end
          end
          final :placed
        end
      end

      describe 'Machine.|' do
        it 'builds a union of machines, nil last' do
          expect((LineItemMachine | PaymentMachine).members).to eq([LineItemMachine, PaymentMachine])
          expect((LineItemMachine | nil).members).to eq([LineItemMachine, nil])
          expect((LineItemMachine | PaymentMachine | nil | LineItemMachine).members).to eq([LineItemMachine, PaymentMachine, nil])
          expect(LineItemMachine | PaymentMachine).to be_a(Sourced::Machine::Starts)
        end

        it 'refuses states and other values, naming both operands' do
          expect { LineItemMachine | OrderMachine::Placed }
            .to raise_error(Sourced::Machine::DefinitionError, /LineItemMachine \| .*OrderMachine::Placed: a union of machines/)
          expect { LineItemMachine | 'x' }.to raise_error(Sourced::Machine::DefinitionError, /a union of machines/)
          expect { OrderMachine::Placed | LineItemMachine }
            .to raise_error(Sourced::Machine::DefinitionError, /OrderMachine::Placed \| .*LineItemMachine: .*LineItemMachine is a machine/)
          expect { OrderMachine::Placed | 'x' }.to raise_error(Sourced::Machine::DefinitionError, /a union of states/)
        end

        it 'is for machines that can be children' do
          expect { machine { initial { command(StartLines, starts: OrderMachine) { nil } } } }
            .to raise_error(Sourced::Machine::DefinitionError, /OrderMachine has no start state/)
          expect { machine { initial { command(StartLines, starts: [LineItemMachine]) { nil } } } }
            .to raise_error(Sourced::Machine::DefinitionError, /starts: takes a machine or a union of them/)
        end
      end

      describe 'without a block' do
        it "starts one child, keyed by the message's payload, the rest its input" do
          parent = machine { initial { command AddLine, starts: LineItemMachine } }

          started, *rest = parent.new(order_id: 'o1').decide(AddLine.new(payload: { order_id: 'o1', line_id: 'l1', sku: 'A1' }))
          expect(rest).to be_empty
          expect(started).to be_a(LineItemMachine::Started)
          expect(started.payload.to_h).to include(order_id: 'o1', line_id: 'l1', input: { sku: 'A1' })
        end

        it 'requires the message to declare the keys the child has beyond ours' do
          expect { machine { initial { command PlaceOrder, starts: LineItemMachine } }.validate! }
            .to raise_error(Sourced::Machine::DefinitionError,
                            /command .*PlaceOrder starts .*LineItemMachine without a block, so it must declare .*line_id/)
        end

        it 'needs a block to start one of several, or none' do
          expect { machine { initial { command AddLine, starts: LineItemMachine | ParcelMachine } } }
            .to raise_error(Sourced::Machine::DefinitionError, /needs a block: nothing chooses which to start/)
          expect { machine { initial { command AddLine, starts: LineItemMachine | nil } } }
            .to raise_error(Sourced::Machine::DefinitionError, /needs a block: nothing decides whether to start one/)
        end
      end

      describe 'with a block' do
        it 'starts: A must start at least one A' do
          parent = parent_with(LineItemMachine)

          expect(started_lines(parent.new(order_id: 'o1').decide(lines('l1', 'l2')))).to eq(%w[l1 l2])
          expect { parent.new(order_id: 'o1').decide(lines) }
            .to raise_error(Sourced::Machine::StartError, /StartLines must start .*LineItemMachine/)
        end

        it 'starts: A | B must start at least one of them, any number' do
          parent = parent_with(LineItemMachine | ParcelMachine)

          expect(started_lines(parent.new(order_id: 'o1').decide(lines('l1', 'l2', 'l3')))).to eq(%w[l1 l2 l3])
          expect { parent.new(order_id: 'o1').decide(lines) }
            .to raise_error(Sourced::Machine::StartError, /must start one of .*LineItemMachine \| .*ParcelMachine/)
        end

        it 'starts: A | nil may start any number' do
          parent = parent_with(LineItemMachine | nil)

          expect(started_lines(parent.new(order_id: 'o1').decide(lines('l1', 'l2')))).to eq(%w[l1 l2])
          placed, *rest = parent.new(order_id: 'o1').decide(lines)
          expect(rest).to be_empty
          expect(placed.payload.target).to eq('placed')
        end

        it 'refuses to start a machine starts: does not declare' do
          parent = machine do
            initial do
              command(StartLines, starts: LineItemMachine) { |_order, _cmd| start PaymentMachine, payment_id: 'p1' }
            end
          end

          expect { parent.new(order_id: 'o1').decide(lines) }
            .to raise_error(Sourced::Machine::StartError, /StartLines starts .*PaymentMachine, which its starts: doesn't declare/)
        end
      end

      it 'starts only from handlers' do
        parent = parent_with(LineItemMachine)
        expect { parent.new(order_id: 'o1').send(:start, LineItemMachine, line_id: 'l1') }
          .to raise_error(Sourced::Machine::Error, /start is for command, on and children_done handlers/)
      end

      it 'starts from on handlers' do
        parent = machine do
          initial { on LineRequested => :requested, starts: LineItemMachine }
          state :requested
        end

        started, moved = parent.new(order_id: 'o1').__decide_on(LineRequested.new(payload: { order_id: 'o1', line_id: 'l1', sku: 'A1' }))
        expect(moved.payload.target).to eq('requested')
        expect(started.payload.to_h).to include(line_id: 'l1', input: { sku: 'A1' })
      end

      describe 'from children_done' do
        let(:keys) { { order_id: 'o1', parcel_id: 'p1' } }

        def parcel_done(parent)
          instance = parent.new(order_id: 'o1')
          instance.evolve([
            ParcelMachine::Started.new(payload: { **keys, trigger: 'start', source: 'initial', target: 'preparing',
                                                          exited: [], entered: %w[preparing preparing.packing] }),
            ParcelMachine::Finished.new(payload: { **keys, final: 'shipped' })
          ])
          instance.__decide_eventless
        end

        it 'starts with a block' do
          parent = machine do
            initial do
              command ShipParcel, starts: ParcelMachine
              children_done(ParcelMachine => :shipped, starts: LineItemMachine) { |_order| start LineItemMachine, line_id: 'returns' }
            end
            final :shipped
          end

          started, moved = parcel_done(parent)
          expect(started.payload.line_id).to eq('returns')
          expect(moved.payload.target).to eq('shipped')
        end

        it 'starts without a block a child keyed by ours alone' do
          child = audit
          parent = machine do
            initial do
              command ShipParcel, starts: ParcelMachine
              children_done ParcelMachine => :shipped, starts: child
            end
            final :shipped
          end

          started, moved = parcel_done(parent)
          expect(started).to be_a(child::Started)
          expect(started.payload.to_h).to include(order_id: 'o1', input: {})
          expect(moved.payload.target).to eq('shipped')
        end

        it 'has no message to take other keys from' do
          parent = machine do
            initial do
              command ShipParcel, starts: ParcelMachine
              children_done ParcelMachine => :shipped, starts: LineItemMachine
            end
            final :shipped
          end

          expect { parent.validate! }
            .to raise_error(Sourced::Machine::DefinitionError, /children_done .*ParcelMachine starts .*LineItemMachine without a block/)
        end
      end

      it 'tracks children by machine' do
        child = audit
        parent = machine do
          initial do
            command AddLine, starts: LineItemMachine
            command ShipParcel, starts: child
          end
        end
        instance = parent.new(order_id: 'o1')
        instance.evolve(instance.decide(AddLine.new(payload: { order_id: 'o1', line_id: 'l1', sku: 'A1' })))
        instance.evolve(instance.decide(ShipParcel.new(payload: { order_id: 'o1', parcel_id: 'p1' })))

        expect(instance.machine.children(LineItemMachine).map(&:keys)).to eq([{ line_id: 'l1' }])
        expect(instance.machine.children.keys).to eq([LineItemMachine, child])
        expect(instance.machine.children(child).map(&:state)).to eq([child::Auditing])
      end

      it 'keeps one spec per machine, in one role' do
        parent = machine do
          initial do
            command AddLine, starts: LineItemMachine
            command(StartLines, starts: LineItemMachine | nil) { |_order, _cmd| nil }
          end
        end
        expect(parent.tree.children_specs.keys).to eq([LineItemMachine])

        expect do
          machine do
            initial { command RequestPayment => :awaiting }
            state(:awaiting) { start(LineItemMachine) { payload { |_, _| { line_id: 'l1' } } } }
            state(:other) { command AddLine, starts: LineItemMachine }
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /LineItemMachine is started by a state, so handlers can't start it too/)
      end

      it 'stops started children where a state stops them' do
        parent = machine do
          initial { command AddLine, starts: LineItemMachine }
          state(:placed) { stops LineItemMachine }
        end

        expect(parent.tree.children_specs.fetch(LineItemMachine).stop_on.map(&:path)).to eq(['placed'])
      end
    end

    describe 'registration' do
      let(:config) { Sourced::Config.build }

      def machine(*keys, &block)
        klass = Class.new(Sourced::Machine)
        stub_const("MachineChildrenTest::Spec#{klass.object_id}", klass)
        klass.partition_by(*(keys.empty? ? %i[order_id] : keys))
        klass.class_eval(&block)
        klass
      end

      def registered?(reactor) = config.declared?(Sourced::Config.reactor_key(reactor))

      it 'returns the machine and the children its handlers and states start' do
        expect(OrderMachine.to_sourced_reactors).to eq([OrderMachine, LineItemMachine])
        expect(BillingMachine.to_sourced_reactors).to eq([BillingMachine, PaymentMachine])
        expect(ParcelMachine.to_sourced_reactors).to eq([ParcelMachine])
      end

      it 'registers a machine with its children, not the machines it mounts' do
        node = Sourced::Config.register(config, ShippingMachine)

        expect(node.path).to eq(Sourced::Config.reactor_key(ShippingMachine))
        expect(registered?(ParcelMachine)).to be(true)
        expect(registered?(PackingMachine)).to be(false)
      end

      it "registers children's children" do
        mid = machine(:order_id, :line_id) do
          started(:added) { command AddLine, starts: LineItemMachine }
          stopped :cancelled
        end
        top = machine { initial { command AddLine, starts: mid } }

        Sourced::Config.register(config, top)

        expect([top, mid, LineItemMachine].map { |m| registered?(m) }).to eq([true, true, true])
      end

      it 'registers a child that starts its parent back once' do
        parent = machine do
          started :opened
          stopped :closed
        end
        child = machine do
          started :added
          stopped :cancelled
        end
        child.class_eval { state(:adding) { command PlaceOrder, starts: parent } }
        parent.class_eval { state(:adding) { command PlaceOrder, starts: child } }

        Sourced::Config.register(config, parent)

        expect([parent, child].map { |m| registered?(m) }).to eq([true, true])
      end

      it "makes a router's setup! raise for children it doesn't route to" do
        expect { Sourced::Router.new(store:, reactors: [OrderMachine]).setup! }
          .to raise_error(Sourced::Router::MissingReactorsError, /OrderMachine needs .*LineItemMachine registered with this router too/)
        expect(Sourced::Router.new(store:, reactors: [OrderMachine, LineItemMachine]).setup!).to be_a(Sourced::Router)
      end

      it 'skips a child registered by another parent, or on its own' do
        other = machine { initial { command AddLine, starts: LineItemMachine } }

        Sourced::Config.register(config, LineItemMachine)
        Sourced::Config.register(config, OrderMachine)
        Sourced::Config.register(config, other)

        expect([OrderMachine, other, LineItemMachine].map { |m| registered?(m) }).to eq([true, true, true])
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

      it 'keeps Started, Finished and Stop apart from states of the same name' do
        child = machine(:order_id, :line_id) do
          started :started
          state :finished
          stopped :stop
        end

        states = child.declared_states.values_at(:started, :finished, :stop)
        expect(states.map(&:type)).to eq(%w[started finished stop].map { |n| "#{child.type_prefix}.runtime.#{n}" })
        expect([child::Started, child::Finished, child::Stop]).to all(satisfy { |klass| !(klass < Sourced::Machine::State) })
      end

      it 'rejects stopped when its constants are taken' do
        expect do
          machine(:order_id, :line_id) do
            const_set(:Started, State(:started))
            started self::Started
            stopped :cancelled
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /stopped generates Started, which is already defined/)
      end

      it 'requires children to partition by the parent keys too' do
        other = Class.new(Sourced::Machine) do
          def self.name = 'MachineChildrenTest::Unrelated'
          partition_by :line_id
          started :added
          stopped :cancelled
        end

        expect { machine { initial { command(PlaceOrder, starts: other) { start other, line_id: 'l1' } } }.validate! }
          .to raise_error(Sourced::Machine::DefinitionError, /must partition by our keys too, missing order_id/)
      end

      it 'validates its children' do
        child = machine(:order_id, :line_id) do
          started :added
          stopped :cancelled
          State(:stray)
        end
        parent = machine { initial { command AddLine, starts: child } }

        expect { parent.validate! }.to raise_error(Sourced::Machine::DefinitionError, /stray.* declared but never placed/)
      end

      it 'validates a child that starts its parent back' do
        parent = machine do
          started :opened
          stopped :closed
        end
        child = machine do
          started :added
          stopped :cancelled
        end
        child.class_eval { state(:adding) { command PlaceOrder, starts: parent } }
        parent.class_eval { state(:adding) { command PlaceOrder, starts: child } }

        expect(parent.validate!).to be(true)
        expect(child.tree).to equal(child.tree)
      end

      it 'requires children to declare started and stopped' do
        no_stop = Class.new(Sourced::Machine) do
          partition_by :order_id, :line_id
          consumer_group 'machine_children_test.no_stop'
          started :added
        end

        expect do
          machine do
            initial { command AddLine, starts: no_stop }
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /has no stopped state/)
      end

      it 'requires start in a state to declare its payload once, and each on_done once' do
        expect { machine { state(:awaiting) { start PaymentMachine } } }
          .to raise_error(Sourced::Machine::DefinitionError, /start .*PaymentMachine needs a block/)
        expect { machine { state(:awaiting) { start(PaymentMachine) { on_done PaymentMachine::Captured => :paid } }; final :paid } }
          .to raise_error(Sourced::Machine::DefinitionError, /start .*PaymentMachine needs payload/)
        expect { machine { state(:awaiting) { start(PaymentMachine) { payload { {} }; payload { {} } } } } }
          .to raise_error(Sourced::Machine::DefinitionError, /PaymentMachine declares payload twice/)
        expect do
          machine do
            state(:awaiting) do
              start PaymentMachine do
                payload { {} }
                on_done PaymentMachine::Captured => :paid
                on_done PaymentMachine::Captured => :failed
              end
            end
            final :paid
            final :failed
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /PaymentMachine maps on_done .*Captured twice/)
        expect { machine { initial { start(PaymentMachine) { payload { {} } } } } }
          .to raise_error(Sourced::Machine::DefinitionError, /can't start children/)
        expect { machine { state(:a) { start(PaymentMachine) { payload { {} } } }; state(:b) { start(PaymentMachine) { payload { {} } } } } }
          .to raise_error(Sourced::Machine::DefinitionError, /PaymentMachine is started by a state twice/)
      end

      it 'checks stops, on_done and children_done' do
        expect { machine { initial { command(CancelOrder => :cancelled) }; final(:cancelled) { stops LineItemMachine } }.validate! }
          .to raise_error(Sourced::Machine::DefinitionError, /cancelled stops .*LineItemMachine, but no handler declares starts:/)
        expect { machine { final(:cancelled) { stops :lines } } }
          .to raise_error(Sourced::Machine::DefinitionError, /stops takes a child machine/)

        bad_done = machine do
          state(:awaiting_payment) do
            start PaymentMachine do
              payload { |_, _| {} }
              on_done PaymentMachine::Requested => :paid
            end
          end
          final :paid
        end
        expect do
          bad_done.validate!
        end.to raise_error(Sourced::Machine::DefinitionError, /on_done .*::Requested isn't a final state/)

        expect do
          machine do
            state(:placed) { children_done(LineItemMachine => :lines_done) }
            final :lines_done
          end.validate!
        end
          .to raise_error(Sourced::Machine::DefinitionError, /children_done .*LineItemMachine: no handler declares starts: .*LineItemMachine/)
        expect { machine { state(:placed) { children_done(lines: :lines_done) } } }
          .to raise_error(Sourced::Machine::DefinitionError, /children_done takes a child machine/)
      end

      it 'requires stopped machines to be named, after partition_by' do
        expect do
          Class.new(Sourced::Machine) do
            partition_by :order_id
            stopped :cancelled
          end
        end
          .to raise_error(Sourced::Machine::DefinitionError, /needs a name/)
      end

      it 'places a root final with its block' do
        child = machine(:order_id, :line_id) do
          started(:added) { command PrintLabel => :cancelled }
          stopped(:cancelled) { entry { |_line, _evt| nil } }
        end

        node = child.nodes.fetch(child::Cancelled)
        expect(node.final?).to be(true)
        expect(node.region.root?).to be(true)
        expect(node.entries.size).to eq(1)
        expect(child.stop_state).to eq(child::Cancelled)
      end

      it 'marks an already-placed root final' do
        child = machine(:order_id, :line_id) do
          started :added
          final(:cancelled) { entry { |_line, _evt| nil } }
          stopped self::Cancelled
        end

        expect(child.nodes.fetch(child::Cancelled).entries.size).to eq(1)
        expect(child.stop_state).to eq(child::Cancelled)
      end

      it 'rejects a block for a state already placed' do
        expect do
          machine(:order_id, :line_id) do
            started :added
            final :cancelled
            stopped(self::Cancelled) { entry { |_line, _evt| nil } }
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /Cancelled is placed twice/)
      end

      it 'requires stopped to be a final of the root region' do
        expect do
          machine(:order_id, :line_id) do
            started(:added) { command PrintLabel => :cancelled }
            state :cancelled
            stopped self::Cancelled
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /stopped .*Cancelled must be a final state of the root region/)

        expect do
          machine(:order_id, :line_id) do
            started :added do
              region(:sub) { final :cancelled }
            end
            stopped self::Cancelled
          end
        end.to raise_error(Sourced::Machine::DefinitionError, /must be a final state of the root region/)
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
