# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
require_relative '../support/machine_helpers'

module MachineExternalEventsTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_external_events_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_external_events_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order)
  Ship = command(:ship)
  CapturePayment = command(:capture_payment, :amount)
  PaymentCaptured = event(:payment_captured, :amount)
  PaymentFailed = event(:payment_failed)
  Noted = event(:noted)
  Stocked = event(:stocked)
  Packed = event(:packed)
  NotifyPaid = command(:notify_paid)
  Unkeyed = Sourced::Event.define('machine_external_events_test.unkeyed') { attribute :sku, String }

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)
    Retrying = State(:retrying)
    Paid = State(:paid)
    Failed = State(:failed)
    Shipped = State(:shipped)

    context { { retries: 0, captured: nil } }
    evolve(PaymentFailed) { |order, _evt| order[:retries] += 1 }
    evolve(PaymentCaptured) { |order, evt| order[:captured] = evt.payload.amount }

    initial { command PlaceOrder => Placed }

    state Placed do
      on PaymentCaptured => Paid
      on PaymentFailed => Retrying | Failed do |order, _evt|
        to(order[:retries] < 3 ? Retrying : Failed)
      end
    end

    state Retrying do
      on PaymentCaptured => Paid
      on PaymentFailed => Retrying | Failed do |order, evt|
        event Noted, order_id: evt.payload.order_id
        to(order[:retries] < 3 ? Retrying : Failed)
      end
    end

    state Paid do
      entry { |_order, _evt| dispatch NotifyPaid }
      command Ship => Shipped
    end

    final Shipped
    final Failed
  end

  # Started by an event; parallel regions taking the same event; eventless steps after it.
  class WarehouseMachine < Sourced::Machine
    partition_by :order_id

    Receiving = State(:receiving)
    Checking = State(:checking)
    Ready = State(:ready)

    initial { on PaymentCaptured => Receiving }

    state Receiving do
      on Stocked => Ready
      region :stock do
        initial { on Stocked => :in_stock }
        final :in_stock
      end
      region :packing do
        initial { on Stocked => :packing }
        state(:packing) { on Packed => :packed }
        final :packed
      end
      done Checking
    end

    state Checking do
      always Ready
    end

    final Ready
  end

  # Mounted: its `on` runs in the mounting machine's partition.
  class PaymentMachine < Sourced::Machine
    Authorizing = State(:authorizing)
    Captured = State(:captured)

    initial { on PaymentFailed => Authorizing }
    state(Authorizing) { on PaymentCaptured => Captured }
    final Captured
  end

  class CheckoutMachine < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)
    Done = State(:done)

    initial { command PlaceOrder => Placed }
    state(Placed) { region :payment, PaymentMachine, on_done: { PaymentMachine::Captured => Done } }
    final Done
  end

  class Payments < Sourced::Decider
    partition_by :order_id

    command CapturePayment do |_context, cmd|
      event PaymentCaptured, order_id: cmd.payload.order_id, amount: cmd.payload.amount
    end
  end
end

RSpec.describe 'Sourced::Machine external events' do
  include MachineHelpers

  MX = MachineExternalEventsTest

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })
  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, order_id: 'o1') })
  def guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 0)

  # Decided events from handling +new_messages+ after +history+, as the GWT helpers do.
  def appended(machine, history, *new_messages, replaying: false)
    pairs = machine.handle_batch({ order_id: 'o1' }, new_messages, history: Sourced::ReadResult.new(messages: history, guard:), replaying:)
    pairs.flat_map { |actions, _| Array(actions) }.filter_map { |action| action.messages if action in Sourced::Actions::Append }.flatten
  end

  def decided(machine, *commands) = machine.new(order_id: 'o1').tap { |m| commands.each { |cmd| m.decide(cmd) } }
  def history_of(machine, *commands)
    instance = machine.new(order_id: 'o1')
    commands.flat_map { |cmd| instance.decide(cmd) }
  end

  let(:placed) { history_of(MX::OrderMachine, msg(MX::PlaceOrder)) }

  describe 'deciding' do
    it 'moves the machine when an active state declares the event, triggered by its type' do
      captured = msg(MX::PaymentCaptured, amount: '10')

      expect(summary(appended(MX::OrderMachine, placed, captured))).to eq(moves(
        MX::OrderMachine,
        ['machine_external_events_test.payment_captured', { from: 'placed', to: 'paid', exited: %w[placed] }]
      ))
    end

    it 'decides with the event already evolved, and picks the target in a block' do
      failures = Array.new(3) { msg(MX::PaymentFailed) }
      instance = MX::OrderMachine.new(order_id: 'o1')
      instance.evolve(placed)

      targets = failures.map do |failed|
        instance.evolve([failed])
        events = instance.__decide_on(failed)
        events.find { |e| e.class < Sourced::Machine::State }.payload.target
      end
      expect(targets).to eq(%w[retrying retrying failed])
    end

    it 'appends data events from the block, before the transition' do
      instance = MX::OrderMachine.new(order_id: 'o1')
      failed = msg(MX::PaymentFailed)
      instance.evolve(placed + [failed])
      instance.evolve(instance.__decide_on(failed))
      second = msg(MX::PaymentFailed)
      instance.evolve([second])

      expect(instance.__decide_on(second).map(&:class)).to eq([MX::Noted, MX::OrderMachine::Retrying])
    end

    it 'ignores the event while no active state declares it, still evolving it' do
      first = msg(MX::PaymentCaptured, amount: '1')
      paid = placed + [first] + appended(MX::OrderMachine, placed, first)
      late = msg(MX::PaymentCaptured, amount: '99')
      instance = MX::OrderMachine.new(order_id: 'o1')
      instance.evolve(paid + [late])

      expect(instance.machine.value).to eq(MX::OrderMachine::Paid)
      expect(instance.__decide_on(late)).to eq([])
      expect(instance.context[:captured]).to eq('99')
      expect(appended(MX::OrderMachine, paid, late)).to eq([])
    end

    it "doesn't decide when replaying" do
      expect(appended(MX::OrderMachine, placed, msg(MX::PaymentCaptured, amount: '10'), replaying: true)).to eq([])
    end

    it 'can start a machine from its initial position' do
      events = appended(MX::WarehouseMachine, [], msg(MX::PaymentCaptured, amount: '1'))
      expect(summary(events)).to eq(moves(
        MX::WarehouseMachine,
        ['machine_external_events_test.payment_captured', { from: 'initial', to: 'receiving' }]
      ))
    end

    it 'lets every deepest active state take it, in definition order, then runs eventless steps' do
      receiving = appended(MX::WarehouseMachine, [], msg(MX::PaymentCaptured, amount: '1'))
      stocked = appended(MX::WarehouseMachine, receiving, msg(MX::Stocked))
      expect(stocked.map { |e| e.payload.target }).to eq(%w[receiving.stock.in_stock receiving.packing.packing])

      packed = appended(MX::WarehouseMachine, receiving + stocked, msg(MX::Packed))
      expect(summary(packed)).to eq(moves(
        MX::WarehouseMachine,
        ['machine_external_events_test.packed', { from: 'receiving.packing.packing', to: 'receiving.packing.packed', exited: %w[receiving.packing.packing] }],
        ['done', { from: 'receiving', to: 'checking', exited: %w[receiving.stock.in_stock receiving.packing.packed receiving] }],
        ['always', { from: 'checking', to: 'ready', exited: %w[checking] }]
      ))
    end

    it "runs a mounted machine's on in the mounting machine's partition, appending its classes" do
      placed = history_of(MX::CheckoutMachine, msg(MX::PlaceOrder))
      failed = appended(MX::CheckoutMachine, placed, msg(MX::PaymentFailed))
      expect(failed.map(&:class)).to eq([MX::CheckoutMachine.state_class(MX::PaymentMachine::Authorizing)])

      captured = appended(MX::CheckoutMachine, placed + failed, msg(MX::PaymentCaptured, amount: '1'))
      expect(captured.map { |e| e.payload.target }).to eq(%w[placed.payment.captured done])
    end
  end

  describe 'definition' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        def self.name = 'MachineExternalEventsTest::Checks'
        partition_by :order_id
        instance_eval(&block)
      end
    end

    it 'claims and evolves the events it declares' do
      expect(MX::OrderMachine.handled_messages).to include(MX::PaymentCaptured, MX::PaymentFailed)
      expect(MX::OrderMachine.handled_messages_for_evolve).to include(MX::PaymentCaptured, MX::PaymentFailed)
      expect(MX::OrderMachine.handled_messages_for_evolve.count(MX::PaymentCaptured)).to eq(1)
      expect(MX::OrderMachine.handled_commands).not_to include(MX::PaymentCaptured)
    end

    it 'requires events with the partition keys' do
      unkeyed = machine do
        initial { on MX::Unkeyed => :done }
        final :done
      end
      expect { unkeyed.validate! }.to raise_error(Sourced::Machine::DefinitionError, /on event .*Unkeyed must declare order_id/)
    end

    it "rejects commands, state classes and children's messages" do
      expect { machine { initial { on MX::Ship => :done }; final :done } }
        .to raise_error(Sourced::Machine::DefinitionError, /on takes an event/)
      expect { machine { initial { on MX::OrderMachine::Paid => :done }; final :done } }
        .to raise_error(Sourced::Machine::DefinitionError, /state/)
      child = Class.new(Sourced::Machine) do
        def self.name = 'MachineExternalEventsTest::Child'
        partition_by :order_id, :line_id
        started :added
        stopped :dropped
      end
      expect { machine { initial { on child::Finished => :done }; final :done } }
        .to raise_error(Sourced::Machine::DefinitionError, /on_done or children_done/)
    end

    it 'rejects on in final and transient states, twice, and unions without a block' do
      expect { machine { initial { command MX::PlaceOrder => :done }; final(:done) { on MX::Noted => :done } }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /final state .* can't take events/)
      expect { machine { initial { command MX::PlaceOrder => :a }; state(:a) { always :b; on MX::Noted => :b }; final :b }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /transient .* take events/)
      expect { machine { initial { on MX::Noted => :a; on MX::Noted => :a }; final :a } }
        .to raise_error(Sourced::Machine::DefinitionError, /takes .*Noted twice/)
      expect { machine { initial { on MX::Noted => [:a, :b] }; final :a; final :b } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
    end
  end

  describe 'rendering' do
    it 'lists events under on in to_h, marked as events' do
      on = MX::OrderMachine.to_h[:states]['placed'][:on]
      expect(on['machine_external_events_test.payment_captured']).to eq(
        kind: 'event', event: 'machine_external_events_test.payment_captured', label: 'PaymentCaptured',
        targets: ['paid'], optional: false, auto: true, starts: [], starts_optional: false
      )
      expect(MX::OrderMachine.to_h[:states]['paid'][:on]['machine_external_events_test.ship']).to include(kind: 'command')
    end

    it 'draws event edges' do
      expect(MX::OrderMachine.to_mermaid).to include('placed --> paid : on PaymentCaptured')
    end

    it 'describes each on as an automation consuming the event' do
      nodes = Sourced::Topology.build([MX::WarehouseMachine])
      on = nodes.find { |n| n.type == 'automation' && n.name == 'on(receiving.packing.packing, Packed)' }
      expect(on).to have_attributes(
        consumes: ['machine_external_events_test.packed'],
        produces: ['machine_external_events_test.warehouse_machine.runtime.packed']
      )
      expect(nodes.find { |n| n.type == 'automation' && n.name == 'on(initial, PaymentCaptured)' })
        .to have_attributes(produces: ['machine_external_events_test.warehouse_machine.runtime.receiving'])
      expect(nodes.find { |n| n.type == 'event' && n.id == 'machine_external_events_test.packed' }).not_to be_nil
    end
  end

  describe 'store-backed' do
    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
    let(:router) { Sourced::Router.new(store:, reactors: [MX::OrderMachine, MX::Payments]).setup! }

    def run(*messages)
      store.append(messages)
      router.drain
    end

    def paid_count = store.read_all(limit: 100).messages.count { |m| m.instance_of?(MX::OrderMachine::Paid) }

    it 'moves on an event another reactor appends, once, and not again on replay' do
      run(msg(MX::PlaceOrder))
      run(msg(MX::CapturePayment, amount: '10'))

      snapshot = MX::OrderMachine.load(store:, order_id: 'o1')
      expect(snapshot.value).to eq(MX::OrderMachine::Paid)
      expect(snapshot.context[:captured]).to eq('10')
      paid = store.read_all(limit: 100).messages.find { |m| m.instance_of?(MX::OrderMachine::Paid) }
      expect(paid.payload.trigger).to eq(MX::PaymentCaptured.type)
      expect(paid_count).to eq(1)

      store.reset_consumer_group(MX::OrderMachine)
      router.drain
      expect(paid_count).to eq(1)
    end
  end
end
