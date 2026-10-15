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
  Nudge = command(:nudge, :thing_id)
  NotifyThingStarted = command(:notify_thing_started)
  Cleanup = command(:cleanup)

  # Usable standalone, as a child, and as a compound checkpoint.
  class ThingMachine < Sourced::Machine
    partition_by :order_id, :thing_id

    Begun = Checkpoint(:begun)
    Progressed = Checkpoint(:progressed)
    Completed = Checkpoint(:completed)
    Aborted = Checkpoint(:aborted)

    context { {} }
    evolve(ThingBegun) { |thing, evt| thing[:thing_id] = evt.payload.thing_id }

    initial { command BeginThing => Begun }   # standalone only

    start Begun do
      entry { |_context, _evt| dispatch NotifyThingStarted }
      after(60) { |thing, _evt| dispatch Nudge, thing_id: thing.fetch(:thing_id) }
      command Progress => Progressed
      command Nudge => Progressed
    end

    checkpoint Progressed do
      exit { |_context, _evt| dispatch Cleanup }
      command Finish => Completed
    end

    final Completed
    on_stop Aborted
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)
    Thing = Checkpoint(:thing)
    Ready = Checkpoint(:ready)
    Cancelled = Checkpoint(:cancelled)

    context { {} }

    initial { command PlaceOrder => Placed }

    checkpoint Placed do
      command StartThing => Thing do |_context, cmd|
        event ThingBegun, thing_id: cmd.payload.thing_id
      end
    end

    checkpoint Thing, ThingMachine, on_done: { ThingMachine::Completed => Ready } do
      entry { |_context, _evt| dispatch NotifyCompound }
      exit { |_context, _evt| dispatch NotifyLeft }
      command RestartThing => Thing
      command Cancel => Cancelled
    end

    final Ready
    final Cancelled
  end

  # The same machine, invoked as a child elsewhere.
  class InvokingMachine < Sourced::Machine
    partition_by :order_id

    Placed = Checkpoint(:placed)
    Ready = Checkpoint(:ready)

    initial { command PlaceOrder => Placed }
    checkpoint Placed do
      invoke ThingMachine, as: :thing, keys: ->(_context) { { thing_id: 't9' } }, on_done: { ThingMachine::Completed => Ready }
    end
    final Ready
  end

  # The same machine as two compound checkpoints, one after the other.
  class TwiceMachine < Sourced::Machine
    partition_by :order_id

    First = Checkpoint(:first)
    Second = Checkpoint(:second)
    Ready = Checkpoint(:ready)

    initial { command StartThing => First }
    checkpoint First, ThingMachine, on_done: { ThingMachine::Completed => Second }
    checkpoint Second, ThingMachine, on_done: { ThingMachine::Completed => Ready }
    final Ready
  end

  # Three levels: a compound checkpoint whose machine starts at a compound checkpoint.
  StartWork = command(:start_work)
  Approve = command(:approve)
  Withdraw = command(:withdraw)
  Accept = command(:accept)
  Review = command(:review)
  RemindOpen = command(:remind_open)
  RemindDraft = command(:remind_draft)
  NotifyWork = command(:notify_work)
  NotifyOpen = command(:notify_open)
  NotifyDraft = command(:notify_draft)
  LeftOpen = command(:left_open)
  LeftWork = command(:left_work)

  class DraftMachine < Sourced::Machine
    Draft = Checkpoint(:draft)
    Pending = Checkpoint(:pending)
    Accepted = Checkpoint(:accepted)

    start Draft do
      entry { |_context, _evt| dispatch NotifyDraft }
      after(30) { |_context, _evt| dispatch RemindDraft }
      command(RemindDraft) { |_context, _cmd| nil }
      command Approve => Pending
    end

    checkpoint Pending do
      command Withdraw => Draft
      command Accept => Accepted
    end

    final Accepted
  end

  class OpenMachine < Sourced::Machine
    Open = Checkpoint(:open)
    Sent = Checkpoint(:sent)

    start Open, DraftMachine, on_done: { DraftMachine::Accepted => Sent } do
      entry { |_context, _evt| dispatch NotifyOpen }
      exit { |_context, _evt| dispatch LeftOpen }
      after(60) { |_context, _evt| dispatch RemindOpen }
      command(RemindOpen) { |_context, _cmd| nil }
    end

    final Sent
  end

  class WorkMachine < Sourced::Machine
    partition_by :order_id

    Work = Checkpoint(:work)
    Done = Checkpoint(:done)

    initial { command StartWork => Work }

    checkpoint Work, OpenMachine, on_done: { OpenMachine::Sent => Done } do
      entry { |_context, _evt| dispatch NotifyWork }
      exit { |_context, _evt| dispatch LeftWork }
    end

    final Done
  end

end

RSpec.describe 'Sourced::Machine compound checkpoints' do
  include MachineHelpers

  M = MachineCompoundTest

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })

  # A fresh instance of +machine+ that has decided +commands+, in order.
  def decided(*commands, machine: M::OrderMachine) = machine.new(order_id: 'o1').tap { |m| commands.each { |cmd| m.decide(cmd) } }

  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }

  # Expected transitions, as [trigger, { from:, to:, … }] pairs.
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, order_id: 'o1') })

  let(:place) { msg(M::PlaceOrder) }
  let(:start) { msg(M::StartThing, thing_id: 't1') }
  let(:progress) { msg(M::Progress, thing_id: 't1') }

  describe 'definition' do
    let(:tree) { M::OrderMachine.tree }

    it 'places the machine under the checkpoint, starting at its start' do
      compound = tree.root.nodes.fetch(M::OrderMachine::Thing)
      region = compound.regions.first

      expect(compound.machine).to eq(M::OrderMachine)
      expect(region).to be_compound
      expect(region.machine).to eq(M::ThingMachine)
      expect(region.name).to eq(:thing)
      expect(region.start_node.key).to eq(M::ThingMachine::Begun)
      expect(region.nodes.keys).to eq([M::ThingMachine::Begun, M::ThingMachine::Progressed, M::ThingMachine::Completed, M::ThingMachine::Aborted])
      expect(tree.placements(M::ThingMachine::Progressed).map(&:region)).to eq([region])
      expect(tree.placements(M::ThingMachine::Progressed).map(&:path)).to eq(%w[thing.progressed])
    end

    it "accepts the machine's commands, not its initial's or its Stop" do
      expect(M::OrderMachine.handled_commands).to include(M::Progress, M::Nudge, M::Finish, M::Cancel)
      expect(M::OrderMachine.handled_commands).not_to include(M::BeginThing, M::ThingMachine::Stop)
    end
  end

  describe 'runtime' do
    it 'enters the machine at its start, with the compound checkpoint' do
      instance = decided(place)
      events = instance.decide(start)

      expect(summary(events)).to eq(
        summary([msg(M::ThingBegun, thing_id: 't1')]) +
        moves(M::OrderMachine, ['machine_compound_test.start_thing', { from: 'placed', to: 'thing', exited: %w[placed], entered: %w[thing thing.begun] }])
      )
      view = instance.machine
      expect(view.value).to eq(M::OrderMachine::Thing => M::ThingMachine::Begun)
      expect(view.in?(M::OrderMachine::Thing)).to be(true)
      expect(view.in?(M::ThingMachine::Begun)).to be(true)
      expect(view.in?('thing.begun')).to be(true)
      expect(view.entered_by(:thing)).to equal(events.last)
    end

    it "routes the machine's commands and the compound checkpoint's own" do
      instance = decided(place, start)
      expect(instance.handles_command?(msg(M::Cancel))).to be(true)

      events = instance.decide(progress)
      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.progress', { from: 'thing.begun', to: 'thing.progressed', exited: %w[thing.begun] }]
      ))
      expect(instance.machine.value).to eq(M::OrderMachine::Thing => M::ThingMachine::Progressed)
    end

    it 'runs both entries and timers when entered, outermost first' do
      instance = decided(place)
      transitioned = instance.decide(start).last
      dispatched = instance.__react(transitioned)

      expect(dispatched.map(&:class)).to eq([M::NotifyCompound, M::NotifyThingStarted, M::Nudge])
      expect(dispatched.last.payload.thing_id).to eq('t1')
      expect(dispatched.last.created_at).to eq(transitioned.created_at + 60)
    end

    it 'exits innermost first when the compound checkpoint is left' do
      instance = decided(place, start, progress)
      events = instance.decide(msg(M::Cancel))

      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.cancel', { from: 'thing', to: 'cancelled', exited: %w[thing.progressed thing] }]
      ))
      expect(instance.__react(events.last).map(&:class)).to eq([M::Cleanup, M::NotifyLeft])
    end

    it 'restarts the machine when the compound checkpoint is re-entered' do
      instance = decided(place, start, progress)
      events = instance.decide(msg(M::RestartThing))

      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.restart_thing', { from: 'thing', to: 'thing', exited: %w[thing.progressed thing], entered: %w[thing thing.begun] }]
      ))
      expect(instance.machine.value).to eq(M::OrderMachine::Thing => M::ThingMachine::Begun)
      expect(instance.__react(events.last).map(&:class))
        .to eq([M::Cleanup, M::NotifyLeft, M::NotifyCompound, M::NotifyThingStarted, M::Nudge])
    end

    it "maps the machine's final onto ours" do
      events = decided(place, start, progress).decide(msg(M::Finish, thing_id: 't1'))

      expect(summary(events)).to eq(moves(
        M::OrderMachine,
        ['machine_compound_test.finish', { from: 'thing.progressed', to: 'thing.completed', exited: %w[thing.progressed] }],
        ['on_done', { from: 'thing.completed', to: 'ready', exited: %w[thing.completed thing] }]
      ))
    end

    it "keeps the start checkpoint's timer valid while the compound checkpoint is active" do
      instance = decided(place)
      nudge = instance.__react(instance.decide(start).last).last

      expect(instance.handles_command?(nudge)).to be(true)
      instance.decide(msg(M::Cancel))
      expect(instance.handles_command?(nudge)).to be(false)
    end
  end

  describe 'a compound start that is itself compound' do
    def work(*commands) = decided(msg(M::StartWork), *commands, machine: M::WorkMachine)

    it 'enters every level, and runs entry actions outermost first' do
      instance = M::WorkMachine.new(order_id: 'o1')
      events = instance.decide(msg(M::StartWork))

      expect(summary(events)).to eq(moves(
        M::WorkMachine,
        ['machine_compound_test.start_work', { from: 'initial', to: 'work', entered: %w[work work.open work.open.draft] }]
      ))
      expect(instance.machine.value).to eq(M::WorkMachine::Work => { M::OpenMachine::Open => M::DraftMachine::Draft })
      expect(instance.__react(events.last).map(&:class))
        .to eq([M::NotifyWork, M::NotifyOpen, M::RemindOpen, M::NotifyDraft, M::RemindDraft])
    end

    it "re-enters only below the compound when a command inside it targets the compound's start" do
      instance = M::WorkMachine.new(order_id: 'o1')
      entered = instance.decide(msg(M::StartWork)).last
      remind_open = instance.__react(entered).find { |cmd| M::RemindOpen === cmd }
      instance.decide(msg(M::Approve))
      events = instance.decide(msg(M::Withdraw))

      expect(summary(events)).to eq(moves(
        M::WorkMachine,
        ['machine_compound_test.withdraw', { from: 'work.open.pending', to: 'work.open.draft', exited: %w[work.open.pending] }]
      ))
      expect(instance.__react(events.last).map(&:class)).to eq([M::NotifyDraft, M::RemindDraft])
      expect(instance.machine.entered_by(:work)).to equal(entered)
      expect(instance.handles_command?(remind_open)).to be(true)
    end

    it "refuses the start's timer from an entry the inner region has since left" do
      instance = M::WorkMachine.new(order_id: 'o1')
      transitioned = instance.decide(msg(M::StartWork)).last
      remind_draft = instance.__react(transitioned).last
      expect(remind_draft).to be_a(M::RemindDraft)

      instance.decide(msg(M::Approve))
      instance.decide(msg(M::Withdraw))

      expect(instance.handles_command?(remind_draft)).to be(false)
    end

    it 'escalates through on_done at each level, in the deciding decision' do
      events = work(msg(M::Approve)).decide(msg(M::Accept))

      expect(summary(events)).to eq(moves(
        M::WorkMachine,
        ['machine_compound_test.accept', { from: 'work.open.pending', to: 'work.open.accepted', exited: %w[work.open.pending] }],
        ['on_done', { from: 'work.open.accepted', to: 'work.sent', exited: %w[work.open.accepted work.open] }],
        ['on_done', { from: 'work.sent', to: 'done', exited: %w[work.sent work] }]
      ))
    end

    it 'exits every level innermost first' do
      instance = work(msg(M::Approve))
      events = instance.decide(msg(M::Accept))

      expect(events.flat_map { |e| instance.__react(e) }.map(&:class)).to eq([M::LeftOpen, M::LeftWork])
    end
  end

  describe "a compound checkpoint's own command targeting its sub-state" do
    it 'enters the sub-state without leaving the compound checkpoint' do
      open_machine = stub_const('MachineCompoundTest::ReviewOpenMachine', Class.new(Sourced::Machine) do
        consumer_group 'machine_compound_test.review_open_machine'
        start :open, M::DraftMachine do
          command M::Review => M::DraftMachine::Pending
        end
      end)
      machine = stub_const('MachineCompoundTest::ReviewMachine', Class.new(Sourced::Machine) do
        partition_by :order_id
        consumer_group 'machine_compound_test.review_machine'
        initial { command M::StartWork => :work }
        checkpoint :work, open_machine
      end)

      instance = decided(msg(M::StartWork), machine:)
      events = instance.decide(msg(M::Review))

      expect(summary(events)).to eq(moves(
        machine,
        ['machine_compound_test.review', { from: 'work.open', to: 'work.open.pending', exited: %w[work.open.draft] }]
      ))
    end
  end

  describe 'the same machine as two compound checkpoints' do
    let(:machine) { M::TwiceMachine }
    let(:finish) { msg(M::Finish, thing_id: 't1') }

    it 'places it under each checkpoint' do
      expect(machine.tree.placements(M::ThingMachine::Progressed).map(&:path)).to eq(%w[first.progressed second.progressed])
    end

    it 'runs each placement in turn' do
      instance = decided(start, progress, machine:)
      expect(instance.machine.value).to eq(M::TwiceMachine::First => M::ThingMachine::Progressed)

      instance.decide(finish)
      expect(instance.machine.value).to eq(M::TwiceMachine::Second => M::ThingMachine::Begun)
      expect(instance.machine.entered_by(:second)).not_to be_nil

      instance.decide(progress)
      instance.decide(finish)
      expect(instance.machine.value).to eq(M::TwiceMachine::Ready)
    end
  end

  describe 'rendering' do
    let(:machine) { M::OrderMachine }

    it 'describes the compound checkpoint in to_h, with its start as its initial state' do
      compound = machine.to_h[:states]['thing']

      expect(compound[:type]).to eq('compound')
      expect(compound[:id]).to eq('thing')
      expect(compound[:states].keys).to include('begun', 'progressed', 'completed')
      expect(compound[:initial]).to eq('thing.begun')
      expect(compound[:mounts]).to eq([{
        region: 'thing', src: M::ThingMachine.name, on_done: { 'thing.completed' => 'ready' }
      }])
    end

    it 'nests a compound start in to_h' do
      work = M::WorkMachine.to_h[:states]['work']

      expect(work[:initial]).to eq('work.open')
      expect(work[:states]['open'][:type]).to eq('compound')
      expect(work[:states]['open'][:initial]).to eq('work.open.draft')
    end

    it "gives an instance's value with ids" do
      value = decided(place, start, progress).to_h[:status][:value]
      expect(value).to eq('thing' => 'thing.progressed')
    end

    it 'draws the compound checkpoint as a nested state' do
      mermaid = machine.to_mermaid
      expect(mermaid).to include('state "thing" as thing {')
      expect(mermaid).to include('[*] --> thing_begun')
      expect(mermaid).to include('thing_begun --> thing_progressed : Progress')
      expect(mermaid).to include('thing_completed --> ready : on_done')
    end

    it 'builds a topology' do
      nodes = Sourced::Topology.build([machine])
      ids = nodes.map(&:id)
      expect(ids.uniq.size).to eq(ids.size)

      commands = nodes.select { |n| n.type == 'command' }
      expect(commands.map(&:name)).to include(M::Progress.name, M::Cancel.name)
      expect(commands.find { |n| n.name == M::StartThing.name }.produces)
        .to contain_exactly(M::OrderMachine::Thing.type, M::ThingBegun.type)
    end
  end

  describe 'the same machine as a child' do
    it 'is invoked with its own partition, at its start' do
      events = M::InvokingMachine.new(order_id: 'o1').decide(msg(M::PlaceOrder))

      expect(summary(events)).to eq(
        moves(M::InvokingMachine, ['machine_compound_test.place_order', { from: 'initial', to: 'placed' }]) +
        summary([M::ThingMachine::Started.new(payload: {
          order_id: 'o1', thing_id: 't9', input: {}, trigger: 'start', source: 'initial', target: 'begun', exited: [], entered: %w[begun]
        })])
      )
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        partition_by :order_id
        consumer_group 'machine_compound_test.definition_check'
        instance_eval(&block)
      end
    end

    it 'rejects a machine with no start' do
      no_start = Class.new(Sourced::Machine) do
        consumer_group 'machine_compound_test.no_start'
        final :done
      end
      expect { machine { checkpoint :thing, no_start } }.to raise_error(Sourced::Machine::DefinitionError, /has no start checkpoint/)
    end

    it 'rejects regions in a compound checkpoint, and on_done without a machine' do
      expect { machine { checkpoint(:thing, M::ThingMachine) { region(:x) { initial } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /compound checkpoint, so it can't have regions/)
      expect { machine { checkpoint :placed, on_done: { M::ThingMachine::Completed => :ready } } }
        .to raise_error(Sourced::Machine::DefinitionError, /on_done is for a compound checkpoint/)
    end
  end
end
