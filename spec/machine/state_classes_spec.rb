# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachineStateClassesTest
  class OrderMachine < Sourced::Machine
    partition_by :order_id

    PlaceOrder = Command(:place_order)
    Placed = State(:placed)

    class PaymentMachine < Sourced::Machine::Part
      part_of OrderMachine

      StartPayment = Command(:start_payment)
      Capture = Command(:capture)

      Authorizing = State(:authorizing)
      Captured = State(:captured)

      initial { command StartPayment => Authorizing }
      state(Authorizing) { command Capture => Captured }
      final Captured
    end

    class ReviewMachine < Sourced::Machine::Part
      part_of OrderMachine

      Review = Command(:review)
      Reviewed = State(:reviewed)

      initial { command Review => Reviewed }
      final Reviewed
    end

    initial { command PlaceOrder => Placed }
    state(Placed) do
      region :payment, PaymentMachine
      region :review, ReviewMachine
    end
  end

  # One part mounted twice: its states are shared, its placements told apart by path.
  class SplitMachine < Sourced::Machine
    partition_by :order_id

    Open = Command(:open)
    Opened = State(:opened)

    class LegMachine < Sourced::Machine::Part
      part_of SplitMachine

      Finish = Command(:finish)
      Done = State(:done)

      initial { command Finish => Done }
      final Done
    end

    initial { command Open => Opened }
    state(Opened) do
      region :left, LegMachine
      region :right, LegMachine
    end
  end

  # A machine with its own partition: started as a child, never mounted.
  class ChargeMachine < Sourced::Machine
    partition_by :order_id, :charge_id

    Requested = State(:requested)
    Charged = State(:charged)

    started(Requested) {}
    final Charged
    stopped :cancelled
  end
end

RSpec.describe 'Sourced::Machine state classes of parts' do
  let(:mod) { MachineStateClassesTest }
  let(:order) { mod::OrderMachine }
  let(:payment) { order::PaymentMachine }

  describe "a part's states" do
    it "are typed under the owner's runtime prefix and the part's" do
      expect(payment::Authorizing.type).to eq('machine_state_classes_test.order_machine.runtime.payment.authorizing')
      expect(order::ReviewMachine::Reviewed.type).to eq('machine_state_classes_test.order_machine.runtime.review.reviewed')
      expect(Sourced::Message.registry[payment::Authorizing.type]).to equal(payment::Authorizing)
    end

    it "take the owner's partition keys, and are their own handles" do
      expect(payment::Authorizing.payload_attribute_names).to contain_exactly(:order_id, :trigger, :source, :target, :exited, :entered)
      expect(payment::Authorizing).to have_attributes(state: payment::Authorizing, machine: payment, state_name: :authorizing)
      expect(payment::Authorizing).to be < Sourced::Machine::State
      expect(Sourced::Machine.handle?(payment::Authorizing)).to be(true)
    end

    it "are the owner's transition classes" do
      expect(order.transition_classes).to eq([
        order::Placed, payment::Authorizing, payment::Captured, order::ReviewMachine::Reviewed
      ])
      expect(order.handled_messages).to include(payment::Authorizing, payment::Captured, order::ReviewMachine::Reviewed)
      expect(order.handled_messages_for_evolve).to include(payment::Authorizing)
    end

    it 'are what the owner appends, evolves and claims' do
      instance = order.new(order_id: 'o1')
      instance.decide(order::PlaceOrder.new(payload: { order_id: 'o1' }))
      events = instance.decide(payment::StartPayment.new(payload: { order_id: 'o1' }))

      expect(events.map(&:class)).to eq([payment::Authorizing])
      expect(payment::Authorizing === events.last).to be(true)
      expect(events.last.payload.target).to eq('placed.payment.authorizing')
      expect(instance.machine.value)
        .to eq(order::Placed => { payment: payment::Authorizing, review: :initial })
    end
  end

  describe 'a part mounted twice' do
    let(:split) { mod::SplitMachine }
    let(:leg) { split::LegMachine }

    it 'shares its state classes, placed at two paths' do
      expect(split.transition_classes).to eq([split::Opened, leg::Done])
      expect(split.tree.placements(leg::Done).map(&:path)).to eq(%w[opened.left.done opened.right.done])
    end

    it 'appends one class, its paths telling the placements apart' do
      instance = split.new(order_id: 'o1')
      instance.decide(split::Open.new(payload: { order_id: 'o1' }))
      events = instance.decide(leg::Finish.new(payload: { order_id: 'o1' }))

      expect(events.map(&:class)).to eq([leg::Done, leg::Done])
      expect(events.map { |evt| evt.payload.target }).to eq(%w[opened.left.done opened.right.done])
      expect(instance.machine.value).to eq(split::Opened => { left: leg::Done, right: leg::Done })
    end
  end

  describe 'on a store' do
    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
    let(:router) { Sourced::Router.new(store:, reactors: [order]).setup! }

    it "replays the owner from its parts' states" do
      router
      Sourced.handle!(order, order::PlaceOrder.new(payload: { order_id: 'o1' }), store:)
      Sourced.handle!(order, payment::StartPayment.new(payload: { order_id: 'o1' }), store:)
      Sourced.handle!(order, payment::Capture.new(payload: { order_id: 'o1' }), store:)

      types = store.read_all(limit: 100).messages.map { |m| m.type.delete_prefix('machine_state_classes_test.order_machine.') }
      expect(types).to include('runtime.payment.authorizing', 'runtime.payment.captured')
      expect(order.load(store:, order_id: 'o1').value)
        .to eq(order::Placed => { payment: payment::Captured, review: :initial })
      expect(order.load(store:, order_id: 'o2').value).to eq(:initial)
    end
  end

  describe 'machines, which have their own partition' do
    it "can't be mounted as a region or a compound state" do
      child = mod::ChargeMachine
      expect { Class.new(Sourced::Machine) { partition_by :order_id; consumer_group 'sc_region'; state(:x) { region :charge, child } } }
        .to raise_error(Sourced::Machine::DefinitionError, /can't mount MachineStateClassesTest::ChargeMachine, a Sourced::Machine/)
      expect { Class.new(Sourced::Machine) { partition_by :order_id; consumer_group 'sc_compound'; state(:x, child) } }
        .to raise_error(Sourced::Machine::DefinitionError, /can't mount MachineStateClassesTest::ChargeMachine.*child \(start, starts:\)/)
    end
  end
end
