# frozen_string_literal: true

require 'spec_helper'
require_relative '../support/machine_helpers'

module MachineAlwaysTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_always_test.#{name}") { ([:trip_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_always_test.#{name}") { ([:trip_id] + attrs).each { |a| attribute a, String } }

  OpenTrip = command(:open_trip)
  AddBooking = command(:add_booking, :status)
  BookingAdded = event(:booking_added, :status)
  Review = command(:review)
  ConfirmTrip = command(:confirm_trip)
  NotifyOrganiser = command(:notify_organiser)
  ShipA = command(:ship_a)
  ShipB = command(:ship_b)

  class GroupState
    attr_reader :bookings

    def initialize = @bookings = []
    def any?(status) = bookings.include?(status)
    def any_refused? = any?('refused')
    def all_confirmed? = bookings.any? && bookings.all?('confirmed')
    def enough? = bookings.size >= 2
  end

  # Guards as callables: a module and an instance.
  module AnyCancelled
    def self.call(trip) = trip.any?('cancelled')
  end

  class AtLeast
    def initialize(count) = @count = count
    def call(trip) = trip.bookings.size >= @count
  end

  class GroupTripMachine < Sourced::Machine
    partition_by :trip_id

    Open = Checkpoint(:open)
    Reviewing = Checkpoint(:reviewing)
    Cancelled = Checkpoint(:cancelled)
    Refused = Checkpoint(:refused)
    Confirmed = Checkpoint(:confirmed)
    AwaitingSuppliers = Checkpoint(:awaiting_suppliers)

    state { GroupState.new }
    evolve(BookingAdded) { |trip, evt| trip.bookings << evt.payload.status }

    initial { command OpenTrip => Open }

    checkpoint Open do
      command(AddBooking) { |_trip, cmd| event BookingAdded, status: cmd.payload.status }
      command Review => Reviewing
    end

    checkpoint Reviewing do
      entry { |_trip, _evt| dispatch NotifyOrganiser }
      always Cancelled, if: ->(trip) { trip.any?('cancelled') }
      always Refused, if: :any_refused?
      always Confirmed, if: :all_confirmed?
      always AwaitingSuppliers
    end

    checkpoint AwaitingSuppliers do
      command ConfirmTrip => Confirmed
    end

    final Cancelled
    final Refused
    final Confirmed
  end

  # A guarded always on a checkpoint that accepts commands: taken once the guard passes.
  class CollectingMachine < Sourced::Machine
    partition_by :trip_id

    Collecting = Checkpoint(:collecting)
    Ready = Checkpoint(:ready)

    state { GroupState.new }
    evolve(BookingAdded) { |trip, evt| trip.bookings << evt.payload.status }

    initial { command OpenTrip => Collecting }

    checkpoint Collecting do
      always Ready, if: :enough?
      command(AddBooking) { |_trip, cmd| event BookingAdded, status: cmd.payload.status }
    end

    final Ready
  end

  # Guarded joins, and a transient checkpoint completing a region before its join.
  class ShipmentMachine < Sourced::Machine
    partition_by :trip_id

    Shipping = Checkpoint(:shipping)
    Delivered = Checkpoint(:delivered)
    Partial = Checkpoint(:partial)

    state { GroupState.new }
    evolve(BookingAdded) { |trip, evt| trip.bookings << evt.payload.status }

    initial do
      command OpenTrip => Shipping
      command(AddBooking) { |_trip, cmd| event BookingAdded, status: cmd.payload.status }
    end

    checkpoint Shipping do
      command(AddBooking) { |_trip, cmd| event BookingAdded, status: cmd.payload.status }

      region :a do
        initial { command ShipA => :a_checking }
        checkpoint(:a_checking) { always :a_shipped }
        final :a_shipped
      end
      region :b do
        initial { command ShipB => :b_shipped }
        final :b_shipped
      end

      done Delivered, if: :all_confirmed?
      done Partial
    end

    final Delivered
    final Partial
  end
end

RSpec.describe 'Sourced::Machine always' do
  include MachineHelpers

  MA = MachineAlwaysTest

  def msg(klass, **attrs) = klass.new(payload: { trip_id: 't1', **attrs })
  def decided(machine, *commands) = machine.new(trip_id: 't1').tap { |m| commands.each { |cmd| m.decide(cmd) } }
  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, trip_id: 't1') })
  def bookings(*statuses) = statuses.map { |status| msg(MA::AddBooking, status:) }

  describe 'always' do
    it 'moves on in the deciding decision, with the first passing guard' do
      instance = decided(MA::GroupTripMachine, msg(MA::OpenTrip), *bookings('confirmed', 'confirmed'))
      events = instance.decide(msg(MA::Review))

      expect(summary(events)).to eq(moves(
        MA::GroupTripMachine,
        ['machine_always_test.review', { from: 'open', to: 'reviewing', exited: %w[open] }],
        ['always', { from: 'reviewing', to: 'confirmed', exited: %w[reviewing] }]
      ))
      expect(instance.machine.value).to eq(MA::GroupTripMachine::Confirmed)
    end

    it 'evaluates guards in order: lambdas, Symbols, then the unguarded fallback' do
      expect(decided(MA::GroupTripMachine, msg(MA::OpenTrip), *bookings('refused', 'cancelled'), msg(MA::Review)).machine.value).to eq(MA::GroupTripMachine::Cancelled)
      expect(decided(MA::GroupTripMachine, msg(MA::OpenTrip), *bookings('confirmed', 'refused'), msg(MA::Review)).machine.value).to eq(MA::GroupTripMachine::Refused)
      expect(decided(MA::GroupTripMachine, msg(MA::OpenTrip), *bookings('confirmed', 'requested'), msg(MA::Review)).machine.value).to eq(MA::GroupTripMachine::AwaitingSuppliers)
    end

    it 'takes any #call(state) as a guard, and renders its name' do
      machine = Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::Callables'
        partition_by :trip_id
        state { MA::GroupState.new }
        evolve(MA::BookingAdded) { |trip, evt| trip.bookings << evt.payload.status }
        initial { command MA::OpenTrip => :open }
        checkpoint(:open) do
          command(MA::AddBooking) { |_trip, cmd| event MA::BookingAdded, status: cmd.payload.status }
          command MA::Review => :reviewing
        end
        checkpoint(:reviewing) do
          always :cancelled, if: MA::AnyCancelled
          always :plenty, if: MA::AtLeast.new(2)
          always :awaiting_suppliers
        end
        final :cancelled
        final :plenty
        final :awaiting_suppliers
      end

      checkpoints = machine.declared_checkpoints
      expect(decided(machine, msg(MA::OpenTrip), *bookings('cancelled'), msg(MA::Review)).machine.value).to eq(checkpoints.fetch(:cancelled))
      expect(decided(machine, msg(MA::OpenTrip), *bookings('confirmed', 'confirmed'), msg(MA::Review)).machine.value).to eq(checkpoints.fetch(:plenty))
      expect(machine.to_h[:states]['reviewing'][:always]).to eq([
        { target: 'cancelled', guard: 'MachineAlwaysTest::AnyCancelled' },
        { target: 'plenty', guard: 'MachineAlwaysTest::AtLeast' },
        { target: 'awaiting_suppliers' }
      ])
    end

    it "runs a transient checkpoint's entry actions when it's claimed" do
      instance = decided(MA::GroupTripMachine, msg(MA::OpenTrip))
      reviewing = instance.decide(msg(MA::Review)).first

      expect(instance.__react(reviewing).map(&:class)).to eq([MA::NotifyOrganiser])
    end

    it 'is re-evaluated after every decision while its checkpoint is active' do
      instance = decided(MA::CollectingMachine, msg(MA::OpenTrip))
      expect(summary(instance.decide(bookings('confirmed').first))).to eq(summary([msg(MA::BookingAdded, status: 'confirmed')]))

      events = instance.decide(bookings('confirmed').first)
      expect(summary(events)).to eq(
        summary([msg(MA::BookingAdded, status: 'confirmed')]) +
        moves(MA::CollectingMachine, ['always', { from: 'collecting', to: 'ready', exited: %w[collecting] }])
      )
    end

    it 'is never decided on replay' do
      instance = MA::GroupTripMachine.new(trip_id: 't1')
      instance.evolve([
        moved(MA::GroupTripMachine, 'machine_always_test.open_trip', from: 'initial', to: 'open', trip_id: 't1'),
        moved(MA::GroupTripMachine, 'machine_always_test.review', from: 'open', to: 'reviewing', exited: %w[open], trip_id: 't1')
      ])

      expect(instance.machine.value).to eq(MA::GroupTripMachine::Reviewing)
    end

    it 'resolves a transient checkpoint before its parent checkpoint joins' do
      instance = decided(MA::ShipmentMachine, msg(MA::OpenTrip), *bookings('confirmed'), msg(MA::ShipB))
      events = instance.decide(msg(MA::ShipA))

      expect(events.map { |e| e.payload.trigger }).to eq(['machine_always_test.ship_a', 'always', 'done'])
      expect(instance.machine.value).to eq(MA::ShipmentMachine::Delivered)
    end

    it 'raises when always checkpoints loop' do
      looping = Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::Looping'
        partition_by :trip_id

        initial { command MA::OpenTrip => :ping }
        checkpoint(:ping) { always :pong }
        checkpoint(:pong) { always :ping }
      end

      expect { looping.new(trip_id: 't1').decide(msg(MA::OpenTrip)) }.to raise_error(Sourced::Machine::Error, /more than 100 joins/)
    end
  end

  describe 'guarded done' do
    it 'takes the first passing guard' do
      instance = decided(MA::ShipmentMachine, msg(MA::OpenTrip), *bookings('confirmed'), msg(MA::ShipB), msg(MA::ShipA))
      expect(instance.machine.value).to eq(MA::ShipmentMachine::Delivered)
    end

    it 'falls back to the unguarded done' do
      instance = decided(MA::ShipmentMachine, msg(MA::OpenTrip), *bookings('requested'), msg(MA::ShipB), msg(MA::ShipA))
      expect(instance.machine.value).to eq(MA::ShipmentMachine::Partial)
    end

    it 'stays when no guard passes' do
      guarded = Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::GuardedDone'
        partition_by :trip_id
        state { MA::GroupState.new }

        initial { command MA::OpenTrip => :shipping }
        checkpoint :shipping do
          region(:a) { initial { command MA::ShipA => :shipped }; final :shipped }
          done :delivered, if: :all_confirmed?
        end
        final :delivered
      end

      instance = decided(guarded, msg(MA::OpenTrip), msg(MA::ShipA))
      expect(instance.machine.value).to eq(guarded.declared_checkpoints.fetch(:shipping) => { a: guarded.declared_checkpoints.fetch(:shipped) })
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        def self.name = 'MachineAlwaysTest::Checks'
        partition_by :trip_id
        instance_eval(&block)
      end
    end

    it "rejects commands and timers on a transient checkpoint" do
      with_command = machine do
        checkpoint(:a) { always :b; command MA::ConfirmTrip => :b }
        final :b
      end
      with_timer = machine do
        checkpoint(:a) { always :b; after(60) { dispatch MA::ConfirmTrip } }
        final :b
      end

      expect { with_command.validate! }.to raise_error(Sourced::Machine::DefinitionError, /a is transient \(always .*b\), so it can't accept commands/i)
      expect { with_timer.validate! }.to raise_error(Sourced::Machine::DefinitionError, /a is transient \(always .*b\), so it can't have timers/i)
    end

    it 'rejects regions and joins on a transient checkpoint, which it leaves before they could run' do
      with_regions = machine do
        checkpoint(:a) { always :b; region(:r) { initial } }
        final :b
      end
      with_done = machine do
        checkpoint(:a) { always :b; done :c }
        final :b
        final :c
      end

      expect { with_regions.validate! }.to raise_error(Sourced::Machine::DefinitionError, /so it can't have regions/)
      expect { with_done.validate! }.to raise_error(Sourced::Machine::DefinitionError, /so it can't join with done or children_done/)
    end

    it 'rejects unreachable always and done' do
      expect { machine { checkpoint(:a) { always :b; always :c }; final :b; final :c } }
        .to raise_error(Sourced::Machine::DefinitionError, /always .*c is unreachable, after an unguarded always/i)
      expect do
        machine do
          checkpoint(:a) { region(:r) { initial { command MA::ShipA => :x }; final :x }; done :b; done :c }
          final :b
          final :c
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /done .*c is unreachable, after an unguarded done/i)
    end

    it 'rejects always on the initial position or a final checkpoint' do
      expect { machine { initial { always :a }; final :a } }
        .to raise_error(Sourced::Machine::DefinitionError, /the initial position is never entered, so no always/)
      expect { machine { final(:a) { always :b }; final :b } }
        .to raise_error(Sourced::Machine::DefinitionError, /final checkpoint .*a can't have always/i)
    end

    it 'rejects guards that are neither a Symbol nor callable' do
      expect { machine { checkpoint(:a) { always :b, if: 'nope' }; final :b } }
        .to raise_error(Sourced::Machine::DefinitionError, /if: takes a Symbol or a #call\(state\), got "nope"/)
    end

    it 'checks always targets as command targets' do
      out_of_reach = machine do
        initial { command MA::OpenTrip => :a }
        checkpoint :a do
          region(:left) { initial { command MA::ShipA => :l }; checkpoint(:l) { always :r } }
          region(:right) { initial { command MA::ShipB => :r }; checkpoint :r }
        end
      end

      expect { out_of_reach.validate! }.to raise_error(Sourced::Machine::DefinitionError, /can only reach/)
    end
  end

  describe 'rendering' do
    it 'describes always and guarded done in to_h' do
      reviewing = MA::GroupTripMachine.to_h[:states]['reviewing']
      expect(reviewing[:always]).to eq([
        { target: 'cancelled', guard: 'guard' },
        { target: 'refused', guard: 'any_refused?' },
        { target: 'confirmed', guard: 'all_confirmed?' },
        { target: 'awaiting_suppliers' }
      ])

      shipping = MA::ShipmentMachine.to_h[:states]['shipping']
      expect(shipping[:on_done]).to eq([{ target: 'delivered', guard: 'all_confirmed?' }, { target: 'partial' }])
      expect(shipping[:states]['a'][:states]['a_checking'][:always]).to eq([{ target: 'shipping.a.a_shipped' }])
      expect(MA::GroupTripMachine.to_h[:states]['open'][:always]).to eq([])
    end

    it 'draws always edges, labelled with their guards' do
      diagram = MA::GroupTripMachine.to_mermaid
      expect(diagram).to include('reviewing --> cancelled : always [guard]')
      expect(diagram).to include('reviewing --> refused : always [any_refused?]')
      expect(diagram).to include('reviewing --> awaiting_suppliers : always')
      expect(MA::ShipmentMachine.to_mermaid).to include('shipping --> delivered : done [all_confirmed?]', 'shipping --> partial : done')
    end

    it "has commands produce the checkpoints their targets' always can reach" do
      nodes = Sourced::Topology.build([MA::GroupTripMachine])
      review = nodes.find { |n| n.type == 'command' && n.id == MA::Review.type }
      types = %i[reviewing cancelled refused confirmed awaiting_suppliers].map { |name| "machine_always_test.group_trip_machine.runtime.#{name}" }

      expect(review.produces).to contain_exactly(*types)
    end

    it 'has a guarded join produce each of its targets' do
      nodes = Sourced::Topology.build([MA::ShipmentMachine])
      join = nodes.find { |n| n.type == 'automation' && n.name == 'done(shipping)' }
      ship_a = nodes.find { |n| n.type == 'command' && n.id == MA::ShipA.type }

      expect(join.produces).to eq(%w[delivered partial].map { |name| "machine_always_test.shipment_machine.runtime.#{name}" })
      expect(ship_a.produces).to contain_exactly(*%w[a_checking a_shipped].map { |name| "machine_always_test.shipment_machine.runtime.#{name}" })
    end
  end
end
