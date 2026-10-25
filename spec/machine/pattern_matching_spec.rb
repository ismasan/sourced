# frozen_string_literal: true

require 'spec_helper'

module MachinePatternMatchingTest
  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Place = Command(:place)
    Pay = Command(:pay)
    Pack = Command(:pack)
    Ship = Command(:ship)
    Cancel = Command(:cancel)

    class Delivery < Sourced::Machine::Part
      part_of OrderMachine

      Progress = Command(:progress)

      Started = State(:started)
      Arrived = State(:arrived)

      started(Started) { command Progress => Arrived }
      final Arrived
    end

    Placed = State(:placed)
    Paid = State(:paid)
    Packed = State(:packed)
    Delivered = State(:delivered)
    Cancelled = State(:cancelled)

    initial { command Place => Placed }

    state Placed do
      command Cancel => Cancelled

      region :payment do
        initial { command Pay => Paid }
        final Paid
      end

      region :shipping do
        initial { command Pack => Packed }
        state(Packed) { command Ship => Delivery }
        state Delivery do
          on_final Delivery::Arrived => Delivered
        end
        final Delivered
      end
    end

    final Cancelled
  end

  # A part with a region of its own, mounted twice: both mounts have a
  # region named :status.
  class TwinMachine < Sourced::Machine
    partition_by :twin_id

    Go = Command(:go)

    class Twin < Sourced::Machine::Part
      part_of TwinMachine

      Begin = Command(:begin)
      Watch = Command(:watch)

      Waiting = State(:waiting)
      Watching = State(:watching)

      initial { command Begin => Watching }

      state Watching do
        region :status do
          initial { command Watch => Waiting }
          final Waiting
        end
      end
    end

    Twins = State(:twins)

    initial { command Go => Twins }

    state Twins do
      region :a, Twin
      region :b, Twin
    end
  end
end

module MachinePatternMatchingTest
  M = OrderMachine

  RSpec.describe 'Pattern matching on a machine' do
    def config(*paths) = M.configuration(paths)

    # The first branch that matches, as a UI renders it.
    def render(machine)
      case machine
      in { payment: M::Paid, shipping: M::Delivered } then :done
      in { shipping: M::Delivery } then :shipping
      in { payment: :initial, shipping: M::Packed | :initial } then :waiting
      in { payment: M::Paid } then :paid
      in M::Cancelled then :cancelled
      in M::Placed then :placed
      else :new
      end
    end

    describe 'a state' do
      it 'matches a configuration where it is active, ancestors included' do
        expect((config('cancelled') in M::Cancelled)).to be(true)
        expect((config('placed', 'placed.shipping.packed') in M::Packed)).to be(true)
        expect((config('placed', 'placed.shipping.packed') in M::Placed)).to be(true)
        expect((config('placed', 'placed.shipping.packed') in M::Paid)).to be(false)
        expect((config in M::Placed)).to be(false)
      end

      it 'matches in case / when too' do
        result = case config('cancelled')
                 when M::Placed then :placed
                 when M::Cancelled then :cancelled
                 end
        expect(result).to eq(:cancelled)
      end

      it 'matches a machine instance and its view' do
        instance = M.new(order_id: 'o1')
        instance.decide(M::Place.new(payload: { order_id: 'o1' }))

        expect((instance in M::Placed)).to be(true)
        expect((instance.machine in M::Placed)).to be(true)
        expect((instance in M::Cancelled)).to be(false)
      end

      it "matches a part's compound state by the part, and the part's states" do
        shipping = config('placed', 'placed.shipping.delivery', 'placed.shipping.delivery.started')
        expect((shipping in M::Delivery)).to be(true)
        expect((shipping in M::Delivery::Started)).to be(true)
        expect((config('placed') in M::Delivery)).to be(false)
      end

      it 'matches its own events, as a class' do
        event = M::Placed.new(payload: { order_id: 'o1', trigger: 'x', source: 'initial', target: 'placed', exited: [], entered: ['placed'] })
        expect(M::Placed === event).to be(true)
        expect(M::Cancelled === event).to be(false)
        expect(M::Placed === M::Placed).to be(true)
        expect(M::Placed === 'placed').to be(false)
      end
    end

    describe 'regions' do
      it 'deconstructs into the active regions, by name, at their current states' do
        expect(config('placed', 'placed.shipping.packed').deconstruct_keys(nil))
          .to eq(root: M::Placed, payment: :initial, shipping: M::Packed)
        expect(config('placed', 'placed.shipping.packed').deconstruct_keys([:shipping])).to eq(shipping: M::Packed)
        expect(config('cancelled').deconstruct_keys([:payment])).to eq({})
      end

      it "names a compound state's region after it" do
        shipping = config('placed', 'placed.shipping.delivery', 'placed.shipping.delivery.started')
        expect(shipping.deconstruct_keys([:shipping])).to eq(shipping: M::Delivery.to_state_handle)
        expect((shipping in { shipping: M::Delivery })).to be(true)
        expect((shipping in { shipping: M::Packed })).to be(false)
      end

      it 'renders the first matching branch' do
        expect(render(config)).to eq(:new)
        expect(render(config('placed'))).to eq(:waiting)
        expect(render(config('placed', 'placed.shipping.packed'))).to eq(:waiting)
        expect(render(config('placed', 'placed.payment.paid'))).to eq(:paid)
        expect(render(config('placed', 'placed.shipping.delivery', 'placed.shipping.delivery.started'))).to eq(:shipping)
        expect(render(config('placed', 'placed.payment.paid', 'placed.shipping.delivered'))).to eq(:done)
        expect(render(config('cancelled'))).to eq(:cancelled)
      end

      it 'matches a machine instance' do
        instance = M.new(order_id: 'o1')
        [M::Place, M::Pay].each { |klass| instance.decide(klass.new(payload: { order_id: 'o1' })) }

        expect(render(instance)).to eq(:paid)
      end

      it "doesn't match a region that isn't active" do
        expect((config('cancelled') in { payment: :initial })).to be(false)
      end

      it 'raises for a region name more than one active region has' do
        instance = TwinMachine.new(twin_id: 't1')
        [TwinMachine::Go, TwinMachine::Twin::Begin].each { |klass| instance.decide(klass.new(payload: { twin_id: 't1' })) }

        expect((instance in { a: TwinMachine::Twin::Watching, b: TwinMachine::Twin::Watching })).to be(true)
        expect { instance in { status: :initial } }
          .to raise_error(Sourced::Machine::AmbiguousRegionError, /2 active regions are named :status/)
      end
    end
  end
end
