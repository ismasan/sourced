# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachineRegionsTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_regions_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_regions_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order, :total)
  OrderPlaced = event(:order_placed, :total)
  CancelOrder = command(:cancel_order)
  OrderCancelled = event(:order_cancelled)
  OrderCompleted = event(:order_completed)
  Pause = command(:pause)
  Paused = event(:paused)
  Resume = command(:resume)

  StartPayment = command(:start_payment, :card)
  PaymentStarted = event(:payment_started)
  PaymentRejected = event(:payment_rejected, :reason)
  PaymentInfoAdded = event(:payment_info_added, :card)
  ExpirePayment = command(:expire_payment)
  PaymentCaptured = event(:payment_captured)
  Abort = command(:abort)

  PackItem = command(:pack_item, :sku, :count)
  ItemPacked = event(:item_packed, :sku)
  ItemsPacked = event(:items_packed)
  DispatchShipment = command(:dispatch_shipment)
  ShipmentDispatched = event(:shipment_dispatched)
  ShippingAborted = event(:shipping_aborted)

  Hold = command(:hold)
  OrderHeld = event(:order_held)
  ShippingHeld = event(:shipping_held)
  Note = command(:note, :text)
  PaymentNoted = event(:payment_noted, :seen)
  ShippingNoted = event(:shipping_noted, :seen)

  ChargeCard = command(:charge_card, :card)
  VoidPayment = command(:void_payment)
  NotifyOrderClosed = command(:notify_order_closed)
  SendReceipt = command(:send_receipt)

  class OrderState
    attr_accessor :total, :card
    attr_reader :packed, :notes

    def initialize = (@packed = []; @notes = [])
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    context { |_| OrderState.new }
    evolve(OrderPlaced) { |order, evt| order.total = evt.payload.total }
    evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }
    evolve(ItemPacked) { |order, evt| order.packed << evt.payload.sku }
    evolve(PaymentNoted) { |order, evt| order.notes << evt.class }
    evolve(ShippingNoted) { |order, evt| order.notes << evt.class }

    initial do
      command PlaceOrder => OrderPlaced
    end

    checkpoint OrderPlaced do
      exit { |_order, _evt| dispatch NotifyOrderClosed }
      command CancelOrder => OrderCancelled
      command Pause => Paused
      command(Hold) { |_order, _cmd| event OrderHeld }

      region :payment do
        initial do
          command StartPayment => PaymentStarted | PaymentRejected do |order, cmd|
            if order.total == '0'
              event PaymentRejected, reason: 'zero total'
            else
              event PaymentStarted
              event PaymentInfoAdded, card: cmd.payload.card
            end
          end
          command(Note) { |order, _cmd| event PaymentNoted, seen: order.notes.size.to_s }
        end

        checkpoint PaymentStarted do
          entry { |order, _evt| dispatch ChargeCard, card: order.card }
          exit { |_order, _evt| dispatch VoidPayment }
          after(3600) { |_order, _evt| dispatch ExpirePayment }
          command ExpirePayment => OrderCancelled          # an ancestor's checkpoint
          command Abort => OrderCancelled
        end

        checkpoint PaymentRejected
        final(PaymentCaptured) do                          # appended by a payments reactor
          entry { |_order, _evt| dispatch SendReceipt }
        end
      end

      region :shipping do
        initial do
          command PackItem => ItemsPacked | nil do |order, cmd|
            event ItemPacked, sku: cmd.payload.sku
            event ItemsPacked if order.packed.size.to_s == cmd.payload.count
          end
          command(Note) { |order, _cmd| event ShippingNoted, seen: order.notes.size.to_s }
          command(Abort) { |_order, _cmd| event ShippingAborted }
        end

        checkpoint ItemsPacked do
          command DispatchShipment => ShipmentDispatched
          command(Hold) { |_order, _cmd| event ShippingHeld }  # deeper than OrderPlaced's Hold
        end

        final ShipmentDispatched
      end

      done OrderCompleted
    end

    checkpoint Paused do
      command Resume => OrderPlaced do |order, _cmd|
        event OrderPlaced, total: order.total
      end
    end

    final OrderCompleted
    final OrderCancelled
  end

RSpec.describe 'Sourced::Machine regions' do
  include Sourced::Testing::RSpec

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })
  def machine_after(*events) = OrderMachine.new(order_id: 'o1').tap { |m| m.evolve(events) }

  let(:placed) { msg(OrderPlaced, total: '100') }
  let(:started) { msg(PaymentStarted) }
  let(:captured) { msg(PaymentCaptured) }
  let(:packed) { msg(ItemsPacked) }
  let(:shipped) { msg(ShipmentDispatched) }

  describe 'configuration' do
    it 'activates every region of a checkpoint, each at its initial position' do
      m = machine_after(placed)

      expect(m.machine.value).to eq(OrderPlaced => { payment: :initial, shipping: :initial })
      expect(m.machine.in?(OrderPlaced)).to be(true)
      expect(m.machine.in?(:payment, :initial)).to be(true)
    end

    it 'moves regions independently' do
      m = machine_after(placed, started)

      expect(m.machine.value).to eq(OrderPlaced => { payment: PaymentStarted, shipping: :initial })
      expect(m.machine.in?(:payment, PaymentStarted)).to be(true)
      expect(m.machine.entered_by(:payment)).to eq(started)
      expect(m.machine.entered_by).to eq(placed)
    end

    it 'deactivates regions when their checkpoint is left' do
      m = machine_after(placed, started, msg(OrderCancelled))

      expect(m.machine.value).to eq(OrderCancelled)
      expect(m.machine.in?(:payment, PaymentStarted)).to be(false)
      expect(m.machine.final?).to be(true)
    end

    it "starts a region's history when its checkpoint is entered again" do
      m = machine_after(placed, started, msg(Paused), placed.with(id: SecureRandom.uuid))

      expect(m.machine.value).to eq(OrderPlaced => { payment: :initial, shipping: :initial })
    end

    it 'ignores checkpoints of inactive regions' do
      m = machine_after(captured, placed)
      expect(m.machine.value).to eq(OrderPlaced => { payment: :initial, shipping: :initial })

      m = machine_after(placed, msg(OrderCancelled), captured)
      expect(m.machine.value).to eq(OrderCancelled)
    end
  end

  describe 'commands' do
    it 'accepts region commands only while the region is active' do
      start = msg(StartPayment, card: 'tok')

      expect(machine_after.handles_command?(start)).to be(false)
      expect(machine_after(placed).handles_command?(start)).to be(true)
      expect(machine_after(placed, started).handles_command?(start)).to be(false)
    end

    it 'decides in the region' do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed)
        .when(StartPayment, order_id: 'o1', card: 'tok')
        .then(started, msg(PaymentInfoAdded, card: 'tok'))
    end

    it 'gives a command to the deepest active checkpoint that accepts it' do
      with_reactor(OrderMachine, order_id: 'o1').given(placed)
        .when(Hold, order_id: 'o1').then(OrderHeld, order_id: 'o1')

      with_reactor(OrderMachine, order_id: 'o1').given(placed).and(packed)
        .when(Hold, order_id: 'o1').then(ShippingHeld, order_id: 'o1')
    end

    it 'runs handlers in sibling regions in definition order, each seeing the ones before' do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed)
        .when(Note, order_id: 'o1', text: 'hi')
        .then(msg(PaymentNoted, seen: '0'), msg(ShippingNoted, seen: '1'))
    end

    it 'skips a handler whose region an earlier handler left' do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed).and(started)
        .when(Abort, order_id: 'o1')
        .then(OrderCancelled, order_id: 'o1')
    end

    it 'can move the machine to an ancestor region checkpoint' do
      m = machine_after(placed, started)
      events = m.decide(msg(ExpirePayment))

      expect(events.map(&:class)).to eq([OrderCancelled])
      expect(m.machine.value).to eq(OrderCancelled)
    end
  end

  describe 'entry and exit actions' do
    it 'exits the active checkpoints innermost first' do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed).and(started)
        .when(OrderCancelled, order_id: 'o1')
        .then(msg(VoidPayment), msg(NotifyOrderClosed))
    end

    it "doesn't run actions for a checkpoint of an inactive region" do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed).and(msg(OrderCancelled))
        .when(captured)
        .then([])
    end
  end

  describe 'joins' do
    it 'appends the join in the decision that completes the last region' do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed).and(captured).and(packed)
        .when(DispatchShipment, order_id: 'o1')
        .then(shipped, msg(OrderCompleted))
    end

    it "doesn't join while a region isn't final" do
      with_reactor(OrderMachine, order_id: 'o1')
        .given(placed).and(packed)
        .when(DispatchShipment, order_id: 'o1')
        .then(shipped)
    end

    it 'decides the join when it claims a checkpoint appended elsewhere, under the guard' do
      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 3)
      history = Sourced::ReadResult.new(messages: [placed, packed, shipped], guard:)

      pairs = OrderMachine.handle_batch({ order_id: 'o1' }, [captured], history:)
      actions, source = pairs.first
      join = actions.find { |a| a.is_a?(Sourced::Actions::Append) && a.guard }

      expect(source).to eq(captured)
      expect(join.messages.map(&:class)).to eq([OrderCompleted])
      expect(join.guard).to eq(guard)
    end

    it "doesn't decide joins while replaying" do
      history = Sourced::ReadResult.new(messages: [placed, packed, shipped], guard: Sourced::ConsistencyGuard.new(conditions: [], last_position: 3))

      expect(OrderMachine.handle_batch({ order_id: 'o1' }, [captured], history:, replaying: true))
        .to eq([[Sourced::Actions::OK, captured]])
    end
  end

  describe 'timers in regions' do
    def timer_from(checkpoint)
      checkpoint.correlate(msg(ExpirePayment).with_metadata(entered_by: checkpoint.id))
    end

    it "go stale when the region's checkpoint entry is left, including by its parent being re-entered" do
      started_again = started.with(id: SecureRandom.uuid)
      m = machine_after(placed, started, msg(Paused), placed.with(id: SecureRandom.uuid), started_again)

      expect(m.handles_command?(timer_from(started))).to be(false)
      expect(m.handles_command?(timer_from(started_again))).to be(true)
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        partition_by :order_id
        class_eval(&block)
      end
    end

    it 'rejects targets in a sibling region' do
      m = machine do
        checkpoint OrderPlaced do
          region(:a) { initial { command Abort => ShipmentDispatched } }
          region(:b) { final ShipmentDispatched }
        end
      end
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /a checkpoint of region :b/)
    end

    it 'rejects done without regions, unreachable, or not buildable from the partition keys' do
      expect { machine { checkpoint(OrderPlaced) { done OrderCompleted }; final OrderCompleted }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /declares done but has no regions/)

      unreachable = machine do
        checkpoint(OrderPlaced) do
          region(:a) { final PaymentCaptured }
          region(:b) { final OrderCompleted }
          done OrderCompleted
        end
      end
      expect { unreachable.validate! }.to raise_error(Sourced::Machine::DefinitionError, /done is .*OrderCompleted, a checkpoint of region :b/)

      unbuildable = machine do
        checkpoint(OrderPlaced) do
          region(:a) { final PaymentCaptured }
          done PaymentRejected
        end
        checkpoint PaymentRejected
      end
      expect { unbuildable.validate! }.to raise_error(Sourced::Machine::DefinitionError, /requires reason/)
    end

    it 'rejects regions on initial positions and final checkpoints, and duplicate region names' do
      expect { machine { initial { region(:a) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /initial position can't have regions/)
      expect { machine { final(OrderCompleted) { region(:a) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /can't have regions/)
      expect { machine { checkpoint(OrderPlaced) { region(:a) { nil }; region(:a) { nil } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /region :a is declared twice/)
    end

    it 'rejects a checkpoint declared in two regions' do
      expect do
        machine do
          checkpoint(OrderPlaced) do
            region(:a) { final PaymentCaptured }
            region(:b) { final PaymentCaptured }
          end
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /PaymentCaptured is declared twice/)
    end
  end

  describe 'running on a store' do
    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
    let(:router) { Sourced::Router.new(store:, reactors: [OrderMachine]).setup! }

    def run(*messages)
      store.append(messages)
      router.drain
    end

    def types = store.read_all(limit: 100).messages.map { |m| m.type.delete_prefix('machine_regions_test.') }
    def value = OrderMachine.load(store:, order_id: 'o1').value

    it 'runs regions to completion and joins on a checkpoint appended elsewhere' do
      run(msg(PlaceOrder, total: '100'))
      run(msg(StartPayment, card: 'tok'))
      expect(value).to eq(OrderPlaced => { payment: PaymentStarted, shipping: :initial })
      expect(types).to include('charge_card')

      run(msg(PackItem, sku: 'A1', count: '1'), msg(DispatchShipment))
      expect(value).to eq(OrderPlaced => { payment: PaymentStarted, shipping: ShipmentDispatched })

      run(captured) # appended by a payments reactor
      expect(value).to eq(OrderCompleted)
      # Claiming PaymentCaptured: the join, then PaymentStarted's exit and PaymentCaptured's entry.
      # Claiming OrderCompleted: OrderPlaced's exit.
      expect(types.last(4)).to eq(%w[order_completed void_payment send_receipt notify_order_closed])
    end
  end
end
end
