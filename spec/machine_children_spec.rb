# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachineChildrenTest
  def self.message(base, name, keys, *attrs)
    base.define("machine_children_test.#{name}") { (keys + attrs).each { |a| attribute a, String } }
  end

  ORDER = %i[order_id].freeze
  LINE = %i[order_id line_id].freeze
  PAYMENT = %i[order_id payment_id].freeze

  def self.command(name, keys, *attrs) = message(Sourced::Command, name, keys, *attrs)
  def self.event(name, keys, *attrs) = message(Sourced::Event, name, keys, *attrs)

  # Order
  PlaceOrder = command(:place_order, ORDER, :currency, :total)
  OrderPlaced = event(:order_placed, ORDER, :currency, :total)
  AddLine = command(:add_line, LINE, :sku)
  CancelOrder = command(:cancel_order, ORDER)
  OrderCancelled = event(:order_cancelled, ORDER)
  LinesDone = event(:lines_done, ORDER)
  LinesPartiallyFailed = event(:lines_partially_failed, ORDER)

  # Line items
  LineAdded = event(:line_added, LINE, :sku)
  StockReserved = command(:stock_reserved, LINE)
  StockUnavailable = command(:stock_unavailable, LINE)
  LineReserved = event(:line_reserved, LINE)
  PickLine = command(:pick_line, LINE)
  LinePicked = event(:line_picked, LINE)
  LineFailed = event(:line_failed, LINE)
  LineCancelled = event(:line_cancelled, LINE)
  ReserveStock = command(:reserve_stock, LINE, :sku, :currency)
  ReleaseStock = command(:release_stock, LINE, :sku)

  # Payments
  RequestPayment = command(:request_payment, ORDER)
  AwaitingPayment = event(:awaiting_payment, ORDER)
  TimeOutPayment = command(:time_out_payment, ORDER)
  PaymentTimedOut = event(:payment_timed_out, ORDER)
  OrderPaid = event(:order_paid, ORDER)
  PaymentFailed = event(:payment_failed, ORDER)
  PaymentRequested = event(:payment_requested, PAYMENT)
  GatewayCaptured = command(:gateway_captured, PAYMENT)
  GatewayDeclined = command(:gateway_declined, PAYMENT)
  PaymentCaptured = event(:payment_captured, PAYMENT)
  PaymentDeclined = event(:payment_declined, PAYMENT)
  PaymentVoided = event(:payment_voided, PAYMENT)
  ChargeCard = command(:charge_card, PAYMENT, :amount)

  LineState = Struct.new(:sku, :currency)

  class LineItemMachine < Sourced::Machine
    partition_by :order_id, :line_id

    state { |_| LineState.new }
    evolve(LineAdded) { |line, evt| line.sku = evt.payload.sku }
    # The parent's fact: never wakes a line, but is in its partition reads
    evolve(OrderPlaced) { |line, evt| line.currency = evt.payload.currency }

    start LineAdded do
      entry { |line, _evt| dispatch ReserveStock, sku: line.sku, currency: line.currency }
      command StockReserved => LineReserved
      command StockUnavailable => LineFailed
    end

    checkpoint LineReserved do
      exit { |line, _evt| dispatch ReleaseStock, sku: line.sku }
      command PickLine => LinePicked
    end

    final LinePicked
    final LineFailed
    on_stop LineCancelled
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    spawns LineItemMachine, as: :lines, stop_on: OrderCancelled

    initial do
      command PlaceOrder => OrderPlaced
    end

    checkpoint OrderPlaced do
      command(AddLine) { |_order, cmd| spawn :lines, line_id: cmd.payload.line_id, sku: cmd.payload.sku }

      children_done :lines => LinesDone | LinesPartiallyFailed do |_order|
        all_picked = machine.children(:lines).all? { |line| line.state == LinePicked }
        all_picked ? event(LinesDone) : event(LinesPartiallyFailed)
      end

      command CancelOrder => OrderCancelled
    end

    final LinesDone
    final LinesPartiallyFailed
    final OrderCancelled
  end

  PaymentState = Struct.new(:amount)

  class PaymentMachine < Sourced::Machine
    partition_by :order_id, :payment_id

    state { |_| PaymentState.new }
    evolve(OrderPlaced) { |pay, evt| pay.amount = evt.payload.total }

    start PaymentRequested do
      entry { |pay, _evt| dispatch ChargeCard, amount: pay.amount }
      command GatewayCaptured => PaymentCaptured
      command GatewayDeclined => PaymentDeclined
    end

    final PaymentCaptured
    final PaymentDeclined
    on_stop PaymentVoided
  end

  class BillingMachine < Sourced::Machine
    partition_by :order_id

    initial do
      command PlaceOrder => OrderPlaced
    end

    checkpoint OrderPlaced do
      command RequestPayment => AwaitingPayment
    end

    checkpoint AwaitingPayment do
      invoke PaymentMachine, as: :payment,
                             keys: ->(_order) { { payment_id: SecureRandom.uuid } },
                             on_done: { PaymentCaptured => OrderPaid, PaymentDeclined => PaymentFailed }
      command TimeOutPayment => PaymentTimedOut
    end

    checkpoint PaymentFailed do
      command RequestPayment => AwaitingPayment
    end

    checkpoint PaymentTimedOut
    final OrderPaid
  end

RSpec.describe 'Sourced::Machine children' do
  include Sourced::Testing::RSpec

  let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }

  def run(router, *messages)
    store.append(messages)
    router.drain
  end

  def log = store.read_all(limit: 200).messages
  def types = log.map { |m| m.type.delete_prefix('machine_children_test.') }

  describe 'Stop' do
    it 'is generated per machine, keyed by its partition keys' do
      expect(LineItemMachine::Stop.type).to eq('machine_children_test.line_item_machine.stop')
      expect(LineItemMachine::Stop.payload_attribute_names).to eq(%i[order_id line_id])
      expect(LineItemMachine.handled_commands).to include(LineItemMachine::Stop)
    end

    it 'is accepted once started, until final, and exits as usual' do
      stop = LineItemMachine::Stop.new(payload: { order_id: 'o1', line_id: 'l1' })
      line = LineItemMachine.new(order_id: 'o1', line_id: 'l1')
      expect(line.handles_command?(stop)).to be(false)

      line.evolve([LineAdded.new(payload: { order_id: 'o1', line_id: 'l1', sku: 'A1' })])
      expect(line.handles_command?(stop)).to be(true)
      expect(line.decide(stop).map(&:class)).to eq([LineCancelled])
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
    def lines = OrderMachine.load(store:, order_id: 'o1').children[:lines].to_h { |ref| [ref.keys[:line_id], ref.state] }

    it 'spawns children as data, tracked by the parent' do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(OrderPlaced, order_id: 'o1', currency: 'EUR', total: '100')
        .when(AddLine, order_id: 'o1', line_id: 'l1', sku: 'A1')
        .then(LineAdded, order_id: 'o1', line_id: 'l1', sku: 'A1')
    end

    it 'runs each child in its own partition, evolving the parent facts it needs' do
      place_and_add_lines

      reserves = log.select { |m| m.is_a?(ReserveStock) }
      expect(reserves.map { |m| [m.payload.line_id, m.payload.currency] }).to contain_exactly(%w[l1 EUR], %w[l2 EUR])
      expect(lines).to eq('l1' => LineAdded, 'l2' => LineAdded)
    end

    it 'joins when every child is final' do
      place_and_add_lines
      run(router, line(StockReserved, 'l1'), line(StockReserved, 'l2'), line(PickLine, 'l1'))
      expect(OrderMachine.load(store:, order_id: 'o1').value).to eq(OrderPlaced)

      run(router, line(PickLine, 'l2'))
      expect(OrderMachine.load(store:, order_id: 'o1').value).to eq(LinesDone)
      expect(lines).to eq('l1' => LinePicked, 'l2' => LinePicked)
    end

    it 'decides the join from the children it ended with' do
      place_and_add_lines
      run(router, line(StockReserved, 'l1'), line(PickLine, 'l1'), line(StockUnavailable, 'l2'))

      expect(OrderMachine.load(store:, order_id: 'o1').value).to eq(LinesPartiallyFailed)
    end

    it 'sends Stop to the live children when it reaches stop_on' do
      place_and_add_lines
      run(router, line(StockReserved, 'l1'), line(StockUnavailable, 'l2'))
      run(router, CancelOrder.new(payload: { order_id: 'o1' }))

      stops = log.select { |m| m.is_a?(LineItemMachine::Stop) }
      expect(stops.map { |m| m.payload.line_id }).to eq(['l1'])
      expect(lines).to eq('l1' => LineCancelled, 'l2' => LineFailed)
      expect(log.select { |m| m.is_a?(ReleaseStock) }.map { |m| m.payload.line_id }).to eq(['l1'])
      expect(OrderMachine.load(store:, order_id: 'o1').value).to eq(OrderCancelled)
    end

    it 'never wakes a child for a parent fact' do
      place_and_add_lines
      claims = []
      allow(LineItemMachine).to receive(:handle_claim).and_wrap_original do |original, claim, **kw|
        claims.concat(claim.messages.map(&:class))
        original.call(claim, **kw)
      end

      run(router, CancelOrder.new(payload: { order_id: 'o1' }))
      expect(claims).to eq([LineItemMachine::Stop, LineItemMachine::Stop, LineCancelled, LineCancelled])
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
      awaiting, requested = log.select { |m| m.is_a?(AwaitingPayment) || m.is_a?(PaymentRequested) }
      expect(requested.causation_id).to eq(awaiting.causation_id)
      expect(payment_ids.size).to eq(1)
      expect(log.find { |m| m.is_a?(ChargeCard) }.payload.amount).to eq('100')
    end

    it "maps the child's final checkpoint onto ours" do
      run(router, gateway(GatewayCaptured, payment_ids.first))

      expect(billing.value).to eq(OrderPaid)
    end

    it 'stops the child when the checkpoint is left, and ignores its late results' do
      payment_id = payment_ids.first
      run(router, TimeOutPayment.new(payload: { order_id: 'o1' }))

      expect(billing.value).to eq(PaymentTimedOut)
      expect(billing.children[:payment].map(&:state)).to eq([PaymentVoided])

      run(router, gateway(GatewayCaptured, payment_id))
      expect(types).not_to include('payment_captured')
      expect(billing.value).to eq(PaymentTimedOut)
    end

    it 'invokes a fresh child on re-entry, and only maps the current one' do
      first = payment_ids.first
      run(router, gateway(GatewayDeclined, first))
      expect(billing.value).to eq(PaymentFailed)

      run(router, RequestPayment.new(payload: { order_id: 'o1' }))
      expect(billing.value).to eq(AwaitingPayment)
      expect(payment_ids.size).to eq(2)
      expect(payment_ids.last).not_to eq(first)

      run(router, gateway(GatewayCaptured, payment_ids.last))
      expect(billing.value).to eq(OrderPaid)
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        partition_by :order_id
        class_eval(&block)
      end
    end

    it 'requires children to partition by the parent keys too' do
      other = Class.new(Sourced::Machine) do
        def self.name = 'MachineChildrenTest::Unrelated'
        partition_by :line_id
        start LineAdded
        on_stop LineCancelled
      end

      expect { machine { spawns other, as: :others }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /must partition by our keys too, missing order_id/)
    end

    it 'requires children to declare start and on_stop' do
      no_stop = Class.new(Sourced::Machine) do
        partition_by :order_id, :line_id
        start LineAdded
      end

      expect { machine { spawns no_stop, as: :x } }.to raise_error(Sourced::Machine::DefinitionError, /has no on_stop/)
    end

    it 'checks stop_on, on_done and children_done' do
      expect { machine { spawns LineItemMachine, as: :lines, stop_on: OrderCancelled }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /stop_on .*OrderCancelled isn't a checkpoint/)

      bad_done = machine do
        checkpoint(AwaitingPayment) do
          invoke PaymentMachine, as: :payment, keys: ->(_) { {} }, on_done: { PaymentRequested => OrderPaid }
        end
        final OrderPaid
      end
      expect { bad_done.validate! }.to raise_error(Sourced::Machine::DefinitionError, /on_done .*PaymentRequested isn't a final checkpoint/)

      expect { machine { checkpoint(OrderPlaced) { children_done(lines: LinesDone) }; final LinesDone }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /children_done :lines doesn't name spawned children/)
    end

    it 'requires on_stop machines to be named, after partition_by' do
      expect { Class.new(Sourced::Machine) { partition_by :order_id; on_stop OrderCancelled } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a name/)
    end

    it 'requires reactions to use types with all partition keys' do
      m = Class.new(Sourced::Machine) do
        partition_by :order_id, :line_id
        reaction(OrderPlaced) { |_s, _e| nil }
      end
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /reaction .*OrderPlaced must declare line_id/)
    end
  end
end
end
