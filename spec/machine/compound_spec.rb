# frozen_string_literal: true

require 'spec_helper'
require_relative '../support/machine_helpers'

module MachineCompoundTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_compound_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_compound_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order)
  StartThing = command(:start_thing, :thing_id)
  RestartThing = command(:restart_thing)
  Cancel = command(:cancel)
  NotifyCompound = command(:notify_compound)
  NotifyLeft = command(:notify_left)

  BeginThing = command(:begin_thing, :thing_id)
  ThingBegun = event(:thing_begun, :thing_id)
  Progress = command(:progress, :thing_id)
  Finish = command(:finish, :thing_id)
  NotifyThingStarted = command(:notify_thing_started)
  Cleanup = command(:cleanup)

  # A machine with its own partition: started as a child, never mounted.
  class ThingMachine < Sourced::Machine
    partition_by :order_id, :thing_id

    Begun = State(:begun)
    Progressed = State(:progressed)
    Completed = State(:completed)
    Aborted = State(:aborted)

    context { {} }

    initial { command BeginThing => Begun }

    started Begun do
      command Progress => Progressed
    end

    state Progressed do
      command Finish => Completed
    end

    final Completed
    stopped Aborted
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    # Mounted as a compound state.
    class ThingMachine < Sourced::Machine::Part
      part_of OrderMachine

      Begin = Command(:begin) { attribute :thing_id, String }
      Progress = Command(:progress) { attribute :thing_id, String }
      Finish = Command(:finish) { attribute :thing_id, String }
      Nudge = Command(:nudge) { attribute :thing_id, String }

      Begun = State(:begun)
      Progressed = State(:progressed)
      Completed = State(:completed)
      Aborted = State(:aborted)

      evolve(ThingBegun) { |order, evt| order[:thing_id] = evt.payload.thing_id }

      initial { command Begin => Begun }   # never reached as a compound state

      started Begun do
        entry { |_context, _evt| dispatch NotifyThingStarted }
        after(60 => Nudge) { |order, _evt| dispatch Nudge, thing_id: order.fetch(:thing_id) }
        command Progress => Progressed
        command Nudge => Progressed
      end

      state Progressed do
        exit { |_context, _evt| dispatch Cleanup }
        command Finish => Completed
      end

      final Completed
      final Aborted
    end

    Placed = State(:placed)
    Ready = State(:ready)
    Cancelled = State(:cancelled)

    context { {} }

    initial { command PlaceOrder => Placed }

    state Placed do
      command StartThing => ThingMachine do |_context, cmd|
        event ThingBegun, thing_id: cmd.payload.thing_id
      end
    end

    state ThingMachine do
      on_final ThingMachine::Completed => Ready
      entry { |_context, _evt| dispatch NotifyCompound }
      exit { |_context, _evt| dispatch NotifyLeft }
      command RestartThing => ThingMachine
      command Cancel => Cancelled
    end

    final Ready
    final Cancelled
  end

  # A child machine, started by a state.
  class InvokingMachine < Sourced::Machine
    partition_by :order_id

    Placed = State(:placed)
    Ready = State(:ready)

    initial { command PlaceOrder => Placed }
    state Placed do
      start ThingMachine do
        payload { |_context, _state| { thing_id: 't9' } }
        on_done ThingMachine::Completed => Ready
      end
    end
    final Ready
  end

  # The same part as the compound states of two parts, one after the other.
  class TwiceMachine < Sourced::Machine
    partition_by :order_id

    class StepsMachine < Sourced::Machine::Part
      part_of TwiceMachine

      Progress = Command(:progress)
      Finish = Command(:finish)

      Begun = State(:begun)
      Progressed = State(:progressed)
      Completed = State(:completed)

      started(Begun) { command Progress => Progressed }
      state(Progressed) { command Finish => Completed }
      final Completed
    end

    class FirstMachine < Sourced::Machine::Part
      part_of TwiceMachine

      started(StepsMachine) { on_final StepsMachine::Completed => :done }
      final :done
    end

    class SecondMachine < Sourced::Machine::Part
      part_of TwiceMachine

      started(StepsMachine) { on_final StepsMachine::Completed => :done }
      final :done
    end

    Ready = State(:ready)

    initial { command StartThing => FirstMachine }
    state(FirstMachine) { on_final FirstMachine::Done => SecondMachine }
    state(SecondMachine) { on_final SecondMachine::Done => Ready }
    final Ready
  end

  # Three levels: a compound state whose part starts at a compound state.
  StartPlanning = command(:start_planning)
  NotifyPlanning = command(:notify_planning)
  NotifyItinerary = command(:notify_itinerary)
  NotifyOutline = command(:notify_outline)
  LeftItinerary = command(:left_itinerary)
  LeftPlanning = command(:left_planning)

  class TripPlanMachine < Sourced::Machine
    partition_by :order_id

    class PlanningMachine < Sourced::Machine::Part
      part_of TripPlanMachine

      class ItineraryMachine < Sourced::Machine::Part
        part_of PlanningMachine

        Propose = Command(:propose)
        Revise = Command(:revise)
        Agree = Command(:agree)
        RemindOutline = Command(:remind_outline)

        Outline = State(:outline)
        Proposed = State(:proposed)
        Agreed = State(:agreed)

        started Outline do
          entry { |_context, _evt| dispatch NotifyOutline }
          after(30 => RemindOutline)
          command(RemindOutline) { |_context, _cmd| nil }
          command Propose => Proposed
        end

        state Proposed do
          command Revise => Outline
          command Agree => Agreed
        end

        final Agreed
      end

      RemindItinerary = Command(:remind_itinerary)

      Finalised = State(:finalised)

      started ItineraryMachine do
        on_final ItineraryMachine::Agreed => Finalised
        entry { |_context, _evt| dispatch NotifyItinerary }
        exit { |_context, _evt| dispatch LeftItinerary }
        after(60 => RemindItinerary)
        command(RemindItinerary) { |_context, _cmd| nil }
      end

      final Finalised
    end

    Done = State(:done)

    initial { command StartPlanning => PlanningMachine }

    state PlanningMachine do
      on_final PlanningMachine::Finalised => Done
      entry { |_context, _evt| dispatch NotifyPlanning }
      exit { |_context, _evt| dispatch LeftPlanning }
    end

    final Done
  end
end

RSpec.describe 'Sourced::Machine compound states' do
  include MachineHelpers

  M = MachineCompoundTest

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })

  # A fresh instance of +machine+ that has decided +commands+, in order.
  def decided(*commands, machine: M::OrderMachine) = machine.new(order_id: 'o1').tap { |m| commands.each { |cmd| m.decide(cmd) } }

  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }

  # Expected transitions, as [trigger, { from:, to:, … }] pairs.
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, order_id: 'o1') })

  let(:thing) { M::OrderMachine::ThingMachine }
  let(:planning_part) { M::TripPlanMachine::PlanningMachine }
  let(:itinerary) { planning_part::ItineraryMachine }
  let(:place) { msg(M::PlaceOrder) }
  let(:start) { msg(M::StartThing, thing_id: 't1') }
  let(:progress) { msg(thing::Progress, thing_id: 't1') }

  describe 'definition' do
    let(:tree) { M::OrderMachine.tree }

    it 'places the part under its compound state, starting at its start' do
      compound = tree.root.nodes.fetch(thing::State)
      region = compound.regions.first

      expect(compound.machine).to eq(M::OrderMachine)
      expect(region).to be_compound
      expect(region.machine).to eq(thing)
      expect(region.name).to eq(:thing)
      expect(region.start_node.key).to eq(thing::Begun)
      expect(region.nodes.keys).to eq([thing::Begun, thing::Progressed, thing::Completed, thing::Aborted])
      expect(tree.placements(thing::Progressed).map(&:region)).to eq([region])
      expect(tree.placements(thing::Progressed).map(&:path)).to eq(%w[thing.progressed])
    end

    it "accepts the part's commands, not its initial's" do
      expect(M::OrderMachine.handled_commands).to include(thing::Progress, thing::Nudge, thing::Finish, M::Cancel)
      expect(M::OrderMachine.handled_commands).not_to include(thing::Begin)
    end

    it "appends the part's states as the owner's" do
      expect(thing::Progressed.type).to eq('machine_compound_test.order_machine.runtime.thing.progressed')
      expect(M::OrderMachine.transition_classes).to include(thing::Begun, thing::Progressed, thing::Completed)
    end
  end

  describe 'runtime' do
    it 'enters the machine at its start, with the compound state' do
      instance = decided(place)
      events = instance.decide(start)

      expect(summary(events)).to eq(
        summary([msg(M::ThingBegun, thing_id: 't1')]) +
        moves(M::OrderMachine, ['machine_compound_test.start_thing', { from: 'placed', to: 'thing', exited: %w[placed], entered: %w[thing thing.begun] }])
      )
      view = instance.machine
      expect(events.last).to be_a(thing::State)
      expect(view.value).to eq(thing::State => thing::Begun)
      expect(view.in?(thing)).to be(true)
      expect(view.in?(thing::State)).to be(true)
      expect(view.in?(thing::Begun)).to be(true)
      expect(view.in?('thing.begun')).to be(true)
      expect(view.entered_by(:thing)).to equal(events.last)
    end

    it "lists the active states' commands, the part's and the compound state's own" do
      instance = decided
      expect(instance.available_commands).to eq(Set[M::PlaceOrder])
      expect(instance.available_commands).to be_frozen
      expect(instance.available_commands).to equal(instance.available_commands)

      instance.decide(place)
      expect(instance.available_commands).to eq(Set[M::StartThing])

      instance.decide(start)
      expect(instance.available_commands).to eq(Set[M::RestartThing, M::Cancel, thing::Progress, thing::Nudge])

      instance.decide(msg(M::Cancel))
      expect(instance.available_commands).to be_empty
    end

    it "routes the part's commands and the compound state's own" do
      instance = decided(place, start)
      expect(instance.handles_command?(msg(M::Cancel))).to be(true)

      events = instance.decide(progress)
      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.order_machine.thing.progress', { from: 'thing.begun', to: 'thing.progressed', exited: %w[thing.begun] }]
      ))
      expect(events.last).to be_a(thing::Progressed)
      expect(instance.machine.value).to eq(thing::State => thing::Progressed)
    end

    it 'runs both entries and timers when entered, outermost first' do
      instance = decided(place)
      transitioned = instance.decide(start).last
      dispatched = instance.__react(transitioned)

      expect(dispatched.map(&:class)).to eq([M::NotifyCompound, M::NotifyThingStarted, thing::Nudge])
      expect(dispatched.last.payload.thing_id).to eq('t1')
      expect(dispatched.last.created_at).to eq(transitioned.created_at + 60)
    end

    it 'exits innermost first when the compound state is left' do
      instance = decided(place, start, progress)
      events = instance.decide(msg(M::Cancel))

      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.cancel', { from: 'thing', to: 'cancelled', exited: %w[thing.progressed thing] }]
      ))
      expect(instance.__react(events.last).map(&:class)).to eq([M::Cleanup, M::NotifyLeft])
    end

    it 'restarts the part when the compound state is re-entered' do
      instance = decided(place, start, progress)
      events = instance.decide(msg(M::RestartThing))

      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.restart_thing', { from: 'thing', to: 'thing', exited: %w[thing.progressed thing], entered: %w[thing thing.begun] }]
      ))
      expect(instance.machine.value).to eq(thing::State => thing::Begun)
      expect(instance.__react(events.last).map(&:class))
        .to eq([M::Cleanup, M::NotifyLeft, M::NotifyCompound, M::NotifyThingStarted, thing::Nudge])
    end

    it "maps the part's final onto ours" do
      events = decided(place, start, progress).decide(msg(thing::Finish, thing_id: 't1'))

      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.order_machine.thing.finish', { from: 'thing.progressed', to: 'thing.completed', exited: %w[thing.progressed] }],
        ['on_final', { from: 'thing.completed', to: 'ready', exited: %w[thing.completed thing] }]
      ))
    end

    it "keeps the start state's timer valid while the compound state is active" do
      instance = decided(place)
      nudge = instance.__react(instance.decide(start).last).last

      expect(instance.handles_command?(nudge)).to be(true)
      instance.decide(msg(M::Cancel))
      expect(instance.handles_command?(nudge)).to be(false)
    end
  end

  describe 'a compound start that is itself compound' do
    def planning(*commands) = decided(msg(M::StartPlanning), *commands, machine: M::TripPlanMachine)

    it 'enters every level, and runs entry actions outermost first' do
      instance = M::TripPlanMachine.new(order_id: 'o1')
      events = instance.decide(msg(M::StartPlanning))

      expect(summary(events)).to eq(moves(
        M::TripPlanMachine,
        ['machine_compound_test.start_planning', { from: 'initial', to: 'planning', entered: %w[planning planning.itinerary planning.itinerary.outline] }]
      ))
      expect(instance.machine.value).to eq(planning_part::State => { itinerary::State => itinerary::Outline })
      expect(instance.__react(events.last).map(&:class))
        .to eq([M::NotifyPlanning, M::NotifyItinerary, planning_part::RemindItinerary, M::NotifyOutline, itinerary::RemindOutline])
    end

    it "re-enters only below the compound when a command inside it targets the compound's start" do
      instance = M::TripPlanMachine.new(order_id: 'o1')
      entered = instance.decide(msg(M::StartPlanning)).last
      remind_itinerary = instance.__react(entered).find { |cmd| planning_part::RemindItinerary === cmd }
      instance.decide(msg(itinerary::Propose))
      events = instance.decide(msg(itinerary::Revise))

      expect(summary(events)).to eq(moves(
        M::TripPlanMachine,
        ['machine_compound_test.trip_plan_machine.planning.itinerary.revise', { from: 'planning.itinerary.proposed', to: 'planning.itinerary.outline', exited: %w[planning.itinerary.proposed] }]
      ))
      expect(instance.__react(events.last).map(&:class)).to eq([M::NotifyOutline, itinerary::RemindOutline])
      expect(instance.machine.entered_by(:planning)).to equal(entered)
      expect(instance.handles_command?(remind_itinerary)).to be(true)
    end

    it "refuses the start's timer from an entry the inner region has since left" do
      instance = M::TripPlanMachine.new(order_id: 'o1')
      transitioned = instance.decide(msg(M::StartPlanning)).last
      remind_outline = instance.__react(transitioned).last
      expect(remind_outline).to be_a(itinerary::RemindOutline)

      instance.decide(msg(itinerary::Propose))
      instance.decide(msg(itinerary::Revise))

      expect(instance.handles_command?(remind_outline)).to be(false)
    end

    it 'escalates through on_final at each level, in the deciding decision' do
      events = planning(msg(itinerary::Propose)).decide(msg(itinerary::Agree))

      expect(summary(events)).to eq(moves(
        M::TripPlanMachine,
        ['machine_compound_test.trip_plan_machine.planning.itinerary.agree', { from: 'planning.itinerary.proposed', to: 'planning.itinerary.agreed', exited: %w[planning.itinerary.proposed] }],
        ['on_final', { from: 'planning.itinerary.agreed', to: 'planning.finalised', exited: %w[planning.itinerary.agreed planning.itinerary] }],
        ['on_final', { from: 'planning.finalised', to: 'done', exited: %w[planning.finalised planning] }]
      ))
    end

    it 'exits every level innermost first' do
      instance = planning(msg(itinerary::Propose))
      events = instance.decide(msg(itinerary::Agree))

      expect(events.flat_map { |e| instance.__react(e) }.map(&:class)).to eq([M::LeftItinerary, M::LeftPlanning])
    end
  end

  describe "a compound state's own command targeting its sub-state" do
    it 'enters the sub-state without leaving the compound state' do
      machine = stub_const('MachineCompoundTest::FastTrackMachine', Class.new(Sourced::Machine) do
        partition_by :order_id
        consumer_group 'machine_compound_test.fast_track_machine'
      end)
      itinerary_part = Class.new(Sourced::Machine::Part) do
        part_of machine, prefix: 'itinerary'
        started(:outline) {}
        final :proposed
      end
      planning_part = Class.new(Sourced::Machine::Part) do
        part_of machine, prefix: 'planning'
        fast_track = const_set(:FastTrack, Command(:fast_track))
        started itinerary_part do
          command fast_track => itinerary_part::Proposed
        end
      end
      machine.initial { command M::StartPlanning => planning_part }
      machine.state planning_part

      instance = decided(msg(M::StartPlanning), machine:)
      events = instance.decide(msg(planning_part::FastTrack))

      expect(summary(events)).to eq(moves(
        machine,
        ['machine_compound_test.fast_track_machine.planning.fast_track',
         { from: 'planning.itinerary', to: 'planning.itinerary.proposed', exited: %w[planning.itinerary.outline] }]
      ))
    end
  end

  describe 'the same part as the compound state of two parts' do
    let(:machine) { M::TwiceMachine }
    let(:steps) { machine::StepsMachine }
    let(:first) { machine::FirstMachine }
    let(:second) { machine::SecondMachine }
    let(:progress) { msg(steps::Progress) }
    let(:finish) { msg(steps::Finish) }

    it 'places it under each, sharing its compound state and its states' do
      expect(machine.tree.placements(steps::State).map(&:path)).to eq(%w[first.steps second.steps])
      expect(machine.tree.placements(steps::Progressed).map(&:path)).to eq(%w[first.steps.progressed second.steps.progressed])
      expect(machine.transition_classes.count(steps::State)).to eq(1)
      expect(machine.transition_classes.count(steps::Progressed)).to eq(1)
    end

    it 'runs each placement in turn' do
      instance = decided(start, progress, machine:)
      expect(instance.machine.value).to eq(first::State => { steps::State => steps::Progressed })

      instance.decide(finish)
      expect(instance.machine.value).to eq(second::State => { steps::State => steps::Begun })
      expect(instance.machine.entered_by(:second)).not_to be_nil

      instance.decide(progress)
      instance.decide(finish)
      expect(instance.machine.value).to eq(M::TwiceMachine::Ready)
    end
  end

  describe 'rendering' do
    let(:machine) { M::OrderMachine }

    it 'describes the compound state in to_h, with its start as its initial state' do
      compound = machine.to_h[:states]['thing']

      expect(compound[:type]).to eq('compound')
      expect(compound[:id]).to eq('thing')
      expect(compound[:states].keys).to include('begun', 'progressed', 'completed')
      expect(compound[:initial]).to eq('thing.begun')
      expect(compound[:mounts]).to eq([{
        region: 'thing', src: thing.name, on_final: { 'thing.completed' => 'ready' }
      }])
    end

    it 'nests a compound start in to_h' do
      planning = M::TripPlanMachine.to_h[:states]['planning']

      expect(planning[:initial]).to eq('planning.itinerary')
      expect(planning[:states]['itinerary'][:type]).to eq('compound')
      expect(planning[:states]['itinerary'][:initial]).to eq('planning.itinerary.outline')
    end

    it "gives an instance's value with ids" do
      value = decided(place, start, progress).to_h[:status][:value]
      expect(value).to eq('thing' => 'thing.progressed')
    end

    it 'draws the compound state as a nested state' do
      mermaid = machine.to_mermaid
      expect(mermaid).to include('state "thing" as thing {')
      expect(mermaid).to include('[*] --> thing_begun')
      expect(mermaid).to include('thing_begun --> thing_progressed : Progress')
      expect(mermaid).to include('thing_completed --> ready : on_final')
    end

    it 'builds a topology' do
      nodes = Sourced::Topology.build([machine])
      ids = nodes.map(&:id)
      expect(ids.uniq.size).to eq(ids.size)

      commands = nodes.select { |n| n.type == 'command' }
      expect(commands.map(&:name)).to include(thing::Progress.name, M::Cancel.name)
      expect(commands.find { |n| n.name == M::StartThing.name }.produces)
        .to contain_exactly(thing::State.type, M::ThingBegun.type)
    end
  end

  describe 'a machine as a child' do
    it 'is started with its own partition, at its start' do
      events = M::InvokingMachine.new(order_id: 'o1').decide(msg(M::PlaceOrder))

      expect(summary(events)).to eq(
        moves(M::InvokingMachine, ['machine_compound_test.place_order', { from: 'initial', to: 'placed' }]) +
        summary([M::ThingMachine::Started.new(payload: {
          order_id: 'o1', thing_id: 't9', input: {}, parent_state: 'placed',
          trigger: 'start', source: 'initial', target: 'begun', exited: [], entered: %w[begun]
        })])
      )
    end
  end

  describe 'definition checks' do
    let(:owner) do
      Class.new(Sourced::Machine) do
        partition_by :order_id
        consumer_group 'machine_compound_test.definition_check'
      end
    end

    # A part of +owner+, named so the chart's +src+ has a name.
    let(:part) do
      stub_const('MachineCompoundTest::DefinitionCheckPart', Class.new(Sourced::Machine::Part)).tap do |klass|
        klass.part_of owner, prefix: 'thing'
        klass.started(:begun) {}
        klass.final :completed
        klass.final :aborted
      end
    end

    def machine(&block) = owner.tap { |klass| klass.instance_exec(part, &block) }

    it 'rejects a part with no start' do
      no_start = Class.new(Sourced::Machine::Part).tap do |klass|
        klass.part_of owner, prefix: 'no_start'
        klass.final :done
      end
      expect { machine { state no_start } }.to raise_error(Sourced::Machine::DefinitionError, /has no start state/)
    end

    it 'rejects a machine, which has its own partition: it starts as a child' do
      expect { machine { state M::ThingMachine } }
        .to raise_error(Sourced::Machine::DefinitionError, /can't mount MachineCompoundTest::ThingMachine, a Sourced::Machine: .*start it as a child/)
    end

    it 'rejects regions in a compound state, and on_final without a part' do
      expect { machine { |thing| state(thing) { region(:x) { initial } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /compound state, so it can't have regions/)
      expect { machine { |thing| state(:placed) { on_final thing::Completed => :ready } } }
        .to raise_error(Sourced::Machine::DefinitionError, /Placed mounts no part, so no on_final; use done/)
    end

    it 'maps each final once, over one or several on_final lines' do
      mapped = machine do |thing|
        state thing do
          on_final thing::Completed => :ready
          on_final thing::Aborted => :cancelled
        end
        final :ready
        final :cancelled
      end
      expect(mapped.to_h.dig(:states, 'thing', :mounts))
        .to eq([{ region: 'thing', src: part.name, on_final: { 'thing.completed' => 'ready', 'thing.aborted' => 'cancelled' } }])
    end

    it 'rejects a final mapped twice' do
      expect do
        machine do |thing|
          state thing do
            on_final thing::Completed => :ready
            on_final thing::Completed => :cancelled
          end
          final :ready
          final :cancelled
        end
      end.to raise_error(Sourced::Machine::DefinitionError, /maps on_final .*Completed twice/)
    end
  end
end
