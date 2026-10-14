# frozen_string_literal: true

require 'spec_helper'
require 'sequel'
require_relative '../support/machine_helpers'

module MachineRegionsTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_regions_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_regions_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order, :total)
  OrderPlaced = event(:order_placed, :total)
  CancelOrder = command(:cancel_order)
  Pause = command(:pause)
  Resume = command(:resume)
  Restart = command(:restart)
  Rush = command(:rush)
  Expedite = command(:expedite)

  StartPayment = command(:start_payment, :card)
  PaymentRejected = event(:payment_rejected, :reason)
  PaymentInfoAdded = event(:payment_info_added, :card)
  ExpirePayment = command(:expire_payment)
  CapturePayment = command(:capture_payment)
  Abort = command(:abort)

  PackItem = command(:pack_item, :sku, :count)
  ItemPacked = event(:item_packed, :sku)
  DispatchShipment = command(:dispatch_shipment)
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

    Placed = Checkpoint(:placed)
    Paused = Checkpoint(:paused)
    Completed = Checkpoint(:completed)
    Cancelled = Checkpoint(:cancelled)
    Charging = Checkpoint(:charging)
    Rejected = Checkpoint(:rejected)
    Captured = Checkpoint(:captured)
    Packed = Checkpoint(:packed)
    Dispatched = Checkpoint(:dispatched)

    state { OrderState.new }
    evolve(OrderPlaced) { |order, evt| order.total = evt.payload.total }
    evolve(PaymentInfoAdded) { |order, evt| order.card = evt.payload.card }
    evolve(ItemPacked) { |order, evt| order.packed << evt.payload.sku }
    evolve(PaymentNoted) { |order, evt| order.notes << evt.class }
    evolve(ShippingNoted) { |order, evt| order.notes << evt.class }

    initial do
      command PlaceOrder => Placed do |_order, cmd|
        event OrderPlaced, total: cmd.payload.total
      end
    end

    checkpoint Placed do
      exit { |_order, _evt| dispatch NotifyOrderClosed }
      command CancelOrder => Cancelled
      command Pause => Paused
      command Restart => Placed                            # re-enters itself
      command Rush => Packed                               # below itself
      command(Hold) { |_order, _cmd| event OrderHeld }

      region :payment do
        initial do
          command StartPayment => Charging | Rejected do |order, cmd|
            if order.total == '0'
              event PaymentRejected, reason: 'zero total'
              to Rejected
            else
              event PaymentInfoAdded, card: cmd.payload.card
              to Charging
            end
          end
          command Expedite => Charging
          command(Note) { |order, _cmd| event PaymentNoted, seen: order.notes.size.to_s }
        end

        checkpoint Charging do
          entry { |order, _evt| dispatch ChargeCard, card: order.card }
          exit { |_order, _evt| dispatch VoidPayment }
          after(3600) { |_order, _evt| dispatch ExpirePayment }
          command ExpirePayment => Cancelled               # an ancestor region's checkpoint
          command Abort => Cancelled
          command CapturePayment => Captured
        end

        checkpoint Rejected
        final(Captured) do
          entry { |_order, _evt| dispatch SendReceipt }
        end
      end

      region :shipping do
        initial do
          command PackItem => Packed | nil do |order, cmd|
            event ItemPacked, sku: cmd.payload.sku
            to Packed if order.packed.size.to_s == cmd.payload.count
          end
          command Expedite => Packed
          command(Note) { |order, _cmd| event ShippingNoted, seen: order.notes.size.to_s }
          command(Abort) { |_order, _cmd| event ShippingAborted }
        end

        checkpoint Packed do
          command DispatchShipment => Dispatched
          command(Hold) { |_order, _cmd| event ShippingHeld }  # deeper than Placed's Hold
        end

        final Dispatched
      end

      done Completed
    end

    checkpoint Paused do
      command Resume => Placed
    end

    final Completed
    final Cancelled
  end

  M = OrderMachine

RSpec.describe 'Sourced::Machine regions' do
  include Sourced::Testing::RSpec
  include MachineHelpers

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })
  def machine_after(*events) = M.new(order_id: 'o1').tap { |m| m.evolve(events.flatten) }

  def given(*events) = events.reduce(with_reactor(M, order_id: 'o1')) { |gwt, evt| gwt.given(evt) }

  def moved_by(trigger, from:, to:, **opts)
    type = case trigger
           in String then trigger
           in Class then trigger.type
           end
    moved(M, type, from:, to:, order_id: 'o1', **opts)
  end

  let(:placed) { moved_by(PlaceOrder, from: 'initial', to: 'placed') }
  let(:history_placed) { [msg(OrderPlaced, total: '100'), placed] }
  let(:charging) { moved_by(StartPayment, from: 'placed.payment.initial', to: 'placed.payment.charging') }
  let(:history_charging) { [msg(PaymentInfoAdded, card: 'tok'), charging] }
  let(:captured) do
    moved_by(CapturePayment, from: 'placed.payment.charging', to: 'placed.payment.captured', exited: %w[placed.payment.charging])
  end
  let(:packed) { moved_by(PackItem, from: 'placed.shipping.initial', to: 'placed.shipping.packed') }
  let(:history_packed) { [msg(ItemPacked, sku: 'A1'), packed] }
  let(:dispatched) do
    moved_by(DispatchShipment, from: 'placed.shipping.packed', to: 'placed.shipping.dispatched', exited: %w[placed.shipping.packed])
  end
  let(:cancelled) do
    moved_by(CancelOrder, from: 'placed', to: 'cancelled', exited: %w[placed.payment.charging placed])
  end
  let(:paused) { moved_by(Pause, from: 'placed', to: 'paused', exited: %w[placed.payment.charging placed]) }
  let(:resumed) { moved_by(Resume, from: 'paused', to: 'placed', exited: %w[paused]) }

  describe 'configuration' do
    it 'activates every region of a checkpoint, each at its initial position' do
      m = machine_after(history_placed)

      expect(m.machine.value).to eq(M::Placed => { payment: :initial, shipping: :initial })
      expect(m.machine.in?(M::Placed)).to be(true)
      expect(m.machine.in?(:payment, :initial)).to be(true)
      expect(m.machine.in?('placed.payment.initial')).to be(true)
    end

    it 'moves regions independently' do
      m = machine_after(history_placed, history_charging)

      expect(m.machine.value).to eq(M::Placed => { payment: M::Charging, shipping: :initial })
      expect(m.machine.in?(:payment, M::Charging)).to be(true)
      expect(m.machine.in?('placed.payment.charging')).to be(true)
      expect(m.machine.entered_by(:payment)).to eq(charging)
      expect(m.machine.entered_by).to eq(placed)
    end

    it 'deactivates regions when their checkpoint is left' do
      m = machine_after(history_placed, history_charging, cancelled)

      expect(m.machine.value).to eq(M::Cancelled)
      expect(m.machine.in?(:payment, M::Charging)).to be(false)
      expect(m.machine.entered_by(:payment)).to be_nil
      expect(m.machine.final?).to be(true)
    end

    it "starts a region's history when its checkpoint is entered again" do
      m = machine_after(history_placed, history_charging, paused, resumed)

      expect(m.machine.value).to eq(M::Placed => { payment: :initial, shipping: :initial })
      expect(m.machine.entered_by).to eq(resumed)
    end

    it 'refuses a transition into an inactive region' do
      expect { machine_after(captured) }.to raise_error(Sourced::Machine::Error, /in an inactive region/)
      expect { machine_after(history_placed, history_charging, cancelled, captured) }
        .to raise_error(Sourced::Machine::Error, /in an inactive region/)
    end
  end

  describe 'commands' do
    it 'accepts region commands only while the region is active' do
      start = msg(StartPayment, card: 'tok')

      expect(machine_after.handles_command?(start)).to be(false)
      expect(machine_after(history_placed).handles_command?(start)).to be(true)
      expect(machine_after(history_placed, history_charging).handles_command?(start)).to be(false)
    end

    it 'decides in the region' do
      given(*history_placed)
        .when(StartPayment, order_id: 'o1', card: 'tok')
        .then(msg(PaymentInfoAdded, card: 'tok'), charging)

      given(msg(OrderPlaced, total: '0'), placed)
        .when(StartPayment, order_id: 'o1', card: 'tok')
        .then(msg(PaymentRejected, reason: 'zero total'), moved_by(StartPayment, from: 'placed.payment.initial', to: 'placed.payment.rejected'))
    end

    it 'gives a command to the deepest active checkpoint that accepts it' do
      given(*history_placed)
        .when(Hold, order_id: 'o1').then(OrderHeld, order_id: 'o1')

      given(*history_placed, *history_packed)
        .when(Hold, order_id: 'o1').then(ShippingHeld, order_id: 'o1')
    end

    it 'runs handlers in sibling regions in definition order, each seeing the ones before' do
      given(*history_placed)
        .when(Note, order_id: 'o1', text: 'hi')
        .then(msg(PaymentNoted, seen: '0'), msg(ShippingNoted, seen: '1'))
    end

    it 'appends one transition per region that moves' do
      given(*history_placed)
        .when(Expedite, order_id: 'o1')
        .then(
          moved_by(Expedite, from: 'placed.payment.initial', to: 'placed.payment.charging'),
          moved_by(Expedite, from: 'placed.shipping.initial', to: 'placed.shipping.packed')
        )
    end

    it 'skips a handler whose region an earlier handler left' do
      given(*history_placed, *history_charging)
        .when(Abort, order_id: 'o1')
        .then(moved_by(Abort, from: 'placed.payment.charging', to: 'cancelled', exited: %w[placed.payment.charging placed]))
    end
  end

  describe 'transition domains' do
    it 'moves within a region without leaving its siblings or its checkpoint' do
      m = machine_after(history_placed, history_charging, history_packed)
      events = m.decide(msg(DispatchShipment))

      expect(events.map(&:payload)).to eq([dispatched.payload])
      expect(m.machine.value).to eq(M::Placed => { payment: M::Charging, shipping: M::Dispatched })
      expect(m.machine.entered_by).to eq(placed)
      expect(m.machine.entered_by(:payment)).to eq(charging)
    end

    it 'exits every active region, innermost first, for an ancestor region target' do
      m = machine_after(history_placed, history_charging, history_packed)
      events = m.decide(msg(ExpirePayment))

      expect(events.map(&:payload)).to eq([
                                            moved_by(ExpirePayment, from: 'placed.payment.charging', to: 'cancelled',
                                                                    exited: %w[placed.payment.charging placed.shipping.packed placed]).payload
                                          ])
      expect(m.machine.value).to eq(M::Cancelled)
    end

    it 'enters a target below the source through its region only' do
      m = machine_after(history_placed, history_charging)
      events = m.decide(msg(Rush))

      expect(events.map(&:payload)).to eq([moved_by(Rush, from: 'placed', to: 'placed.shipping.packed').payload])
      expect(m.machine.value).to eq(M::Placed => { payment: M::Charging, shipping: M::Packed })
      expect(m.machine.entered_by).to eq(placed)
    end

    it 're-enters a parallel checkpoint that targets itself, resetting its regions' do
      m = machine_after(history_placed, history_charging, history_packed)
      events = m.decide(msg(Restart))

      expect(events.map(&:payload)).to eq([
                                            moved_by(Restart, from: 'placed', to: 'placed', exited: %w[placed.payment.charging placed.shipping.packed placed]).payload
                                          ])
      expect(m.machine.value).to eq(M::Placed => { payment: :initial, shipping: :initial })
      expect(m.machine.entered_by).to eq(events.last)
    end

    it 'resets the regions of a parallel checkpoint entered from outside' do
      m = machine_after(history_placed, history_charging, paused)
      events = m.decide(msg(Resume))

      expect(events.map(&:payload)).to eq([resumed.payload])
      expect(m.machine.value).to eq(M::Placed => { payment: :initial, shipping: :initial })
    end
  end

  describe 'entry and exit actions' do
    it 'exits the active checkpoints innermost first' do
      given(*history_placed, *history_charging)
        .when(cancelled)
        .then(msg(VoidPayment), msg(NotifyOrderClosed))
    end

    it 'runs the exits of what a transition left, then the entries of what it entered' do
      given(*history_placed, *history_charging)
        .when(captured)
        .then(msg(VoidPayment), msg(SendReceipt))
    end

    it "runs a region checkpoint's entry with the domain state, and starts its timers" do
      given(*history_placed, msg(PaymentInfoAdded, card: 'tok'))
        .when(charging)
        .then do |result|
          expect(result.messages.map(&:class)).to eq([ChargeCard, ExpirePayment])
          expect(result.messages.first.payload.card).to eq('tok')
        end
    end
  end

  describe 'joins' do
    let(:done) do
      moved_by('done', from: 'placed', to: 'completed',
                       exited: %w[placed.payment.captured placed.shipping.dispatched placed])
    end

    it 'appends the join in the decision that completes the last region' do
      given(*history_placed, *history_charging, captured, *history_packed)
        .when(DispatchShipment, order_id: 'o1')
        .then(dispatched, done)

      given(*history_placed, *history_charging, *history_packed, dispatched)
        .when(CapturePayment, order_id: 'o1')
        .then(captured, done)
    end

    it "doesn't join while a region isn't final" do
      given(*history_placed, *history_packed)
        .when(DispatchShipment, order_id: 'o1')
        .then(dispatched)
    end

    it 'appends the join under the guard, with the transition that completes it' do
      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 3)
      history = Sourced::ReadResult.new(messages: [*history_placed, *history_charging, *history_packed, dispatched], guard:)
      command = msg(CapturePayment)

      pairs = M.handle_batch({ order_id: 'o1' }, [command], history:)
      actions, source = pairs.first
      append = actions.find { |a| Sourced::Actions::Append === a && a.guard }

      expect(source).to eq(command)
      expect(append.messages.map { |m| m.payload.trigger }).to eq([CapturePayment.type, 'done'])
      expect(append.guard).to eq(guard)
    end

    it "doesn't run actions while replaying" do
      history = Sourced::ReadResult.new(
        messages: [*history_placed, *history_charging, *history_packed, dispatched],
        guard: Sourced::ConsistencyGuard.new(conditions: [], last_position: 3)
      )

      expect(M.handle_batch({ order_id: 'o1' }, [captured], history:, replaying: true))
        .to eq([[Sourced::Actions::OK, captured]])
    end
  end

  describe 'timers in regions' do
    def timer_from(transition)
      transition.correlate(msg(ExpirePayment).with_metadata(entered_by: transition.id))
    end

    it "go stale when the region's checkpoint entry is left, including by its parent being re-entered" do
      charging_again = charging.with(id: SecureRandom.uuid)
      m = machine_after(history_placed, history_charging, paused, resumed, charging_again)

      expect(m.handles_command?(timer_from(charging))).to be(false)
      expect(m.handles_command?(timer_from(charging_again))).to be(true)
    end
  end

  describe 'definition checks' do
    def machine(&block)
      stub_const('MachineRegionsTest::Draft', Class.new(Sourced::Machine)).tap do |klass|
        klass.partition_by :order_id
        klass.class_eval(&block)
      end
    end

    it 'rejects targets in a sibling region' do
      m = machine do
        checkpoint :placed do
          region(:a) { initial { command Abort => :dispatched } }
          region(:b) { final :dispatched }
        end
      end
      expect { m.validate! }.to raise_error(Sourced::Machine::DefinitionError, /can only reach region placed\.a/)
    end

    it 'rejects done without regions, or out of reach' do
      expect { machine { checkpoint(:placed) { done :completed }; final :completed }.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /declares done but has no regions/)

      unreachable = machine do
        checkpoint(:placed) do
          region(:a) do
            checkpoint(:paying) do
              region(:inner) { final :captured }
              done :completed
            end
          end
          region(:b) { final :completed }
        end
      end
      expect { unreachable.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /placed\.a\.paying targets .*completed.*can only reach/)
    end

    it 'rejects regions on initial positions and final checkpoints, and duplicate region names' do
      expect { machine { initial { region(:a) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /initial position can't have regions/)
      expect { machine { final(:completed) { region(:a) { nil } } } }.to raise_error(Sourced::Machine::DefinitionError, /can't have regions/)
      expect { machine { checkpoint(:placed) { region(:a) { nil }; region(:a) { nil } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /region :a is declared twice/)
    end

    it 'rejects a checkpoint placed in two regions' do
      expect do
        machine do
          checkpoint(:placed) do
            region(:a) { final :captured }
            region(:b) { final :captured }
          end
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /captured is placed twice/)
    end
  end

  describe 'running on a store' do
    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
    let(:router) { Sourced::Router.new(store:, reactors: [M]).setup! }

    def run(*messages)
      store.append(messages)
      router.drain
    end

    def types = store.read_all(limit: 100).messages.map { |m| m.type.delete_prefix('machine_regions_test.') }
    def value = M.load(store:, order_id: 'o1').value

    it 'runs regions to completion and joins' do
      run(msg(PlaceOrder, total: '100'))
      run(msg(StartPayment, card: 'tok'))
      expect(value).to eq(M::Placed => { payment: M::Charging, shipping: :initial })
      expect(types).to include('charge_card')

      run(msg(PackItem, sku: 'A1', count: '1'), msg(DispatchShipment))
      expect(value).to eq(M::Placed => { payment: M::Charging, shipping: M::Dispatched })

      run(msg(CapturePayment))
      expect(value).to eq(M::Completed)
      # The capture and the join, then Charging's exit, Captured's entry and Placed's exit.
      expect(types.last(6)).to eq(%w[
                                    capture_payment order_machine.transitioned order_machine.transitioned void_payment send_receipt notify_order_closed
                                  ])
    end
  end
end
end
