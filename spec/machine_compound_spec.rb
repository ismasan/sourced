# frozen_string_literal: true

require 'spec_helper'

module MachineCompoundTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_compound_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }
  def self.event(name, *attrs) = Sourced::Event.define("machine_compound_test.#{name}") { ([:order_id] + attrs).each { |a| attribute a, String } }

  PlaceOrder = command(:place_order)
  OrderPlaced = event(:order_placed)
  StartThing = command(:start_thing, :thing_id)
  Cancel = command(:cancel)
  OrderCancelled = event(:order_cancelled)
  OrderReady = event(:order_ready)
  NotifyCompound = command(:notify_compound)
  NotifyLeft = command(:notify_left)

  BeginThing = command(:begin_thing, :thing_id)
  ThingStarted = event(:thing_started, :thing_id)
  Progress = command(:progress, :thing_id)
  ThingProgressed = event(:thing_progressed, :thing_id)
  Finish = command(:finish, :thing_id)
  ThingFinished = event(:thing_finished, :thing_id)
  ThingAborted = event(:thing_aborted, :thing_id)
  Nudge = command(:nudge, :thing_id)
  NotifyThingStarted = command(:notify_thing_started)
  Cleanup = command(:cleanup)

  # Usable standalone, as a child, and as a compound checkpoint.
  class ThingMachine < Sourced::Machine
    partition_by :order_id, :thing_id

    initial { command BeginThing => ThingStarted }   # standalone only

    start ThingStarted do
      entry { |_context, _evt| dispatch NotifyThingStarted }
      after(60) { |_context, evt| dispatch Nudge, thing_id: evt.payload.thing_id }
      command Progress => ThingProgressed
      command Nudge => ThingProgressed
    end

    checkpoint ThingProgressed do
      exit { |_context, _evt| dispatch Cleanup }
      command Finish => ThingFinished
    end

    final ThingFinished
    on_stop ThingAborted
  end

  class OrderMachine < Sourced::Machine
    partition_by :order_id

    initial { command PlaceOrder => OrderPlaced }

    checkpoint OrderPlaced do
      command StartThing => ThingStarted
    end

    checkpoint ThingStarted, ThingMachine, on_done: { ThingFinished => OrderReady } do
      entry { |_context, _evt| dispatch NotifyCompound }
      exit { |_context, _evt| dispatch NotifyLeft }
      command Cancel => OrderCancelled
    end

    final OrderReady
    final OrderCancelled
  end

  # The short form: the event is the machine's start.
  class ShortOrderMachine < Sourced::Machine
    partition_by :order_id

    initial { command StartThing => ThingStarted }
    checkpoint ThingMachine
  end

  # The same machine, invoked as a child elsewhere.
  class InvokingMachine < Sourced::Machine
    partition_by :order_id

    initial { command PlaceOrder => OrderPlaced }
    checkpoint OrderPlaced do
      invoke ThingMachine, as: :thing, keys: ->(_context) { { thing_id: 't9' } }, on_done: { ThingFinished => OrderReady }
    end
    final OrderReady
  end
end

RSpec.describe 'Sourced::Machine compound checkpoints' do
  M = MachineCompoundTest

  def msg(klass, **attrs) = klass.new(payload: { order_id: 'o1', **attrs })
  def after(*events) = M::OrderMachine.new(order_id: 'o1').tap { |m| m.evolve(events) }

  let(:placed) { msg(M::OrderPlaced) }
  let(:started) { msg(M::ThingStarted, thing_id: 't1') }
  let(:progressed) { msg(M::ThingProgressed, thing_id: 't1') }
  let(:cancelled) { msg(M::OrderCancelled) }

  describe 'definition' do
    let(:tree) { M::OrderMachine.tree }

    it "places the machine under the checkpoint, starting at its start" do
      compound = tree.checkpoints[M::ThingStarted]
      region = compound.regions.first

      expect(compound.machine).to eq(M::OrderMachine)
      expect(region).to be_compound
      expect(region.machine).to eq(M::ThingMachine)
      expect(region.start).to eq(M::ThingStarted)
      expect(region.nodes.keys).to eq([M::ThingStarted, M::ThingProgressed, M::ThingFinished, M::ThingAborted])
      expect(region.nodes[M::ThingStarted]).to be_compound_start
      expect(tree.checkpoints[M::ThingProgressed].region).to equal(region)
    end

    it "accepts the machine's commands, not its initial's or its Stop" do
      expect(M::OrderMachine.handled_commands).to include(M::Progress, M::Nudge, M::Finish, M::Cancel)
      expect(M::OrderMachine.handled_commands).not_to include(M::BeginThing, M::ThingMachine::Stop)
    end

    it 'derives the event from the machine in the short form' do
      region = M::ShortOrderMachine.tree.checkpoints[M::ThingStarted].regions.first
      expect(region.machine).to eq(M::ThingMachine)
      expect(region.name).to eq(:thing_machine)
    end
  end

  describe 'runtime' do
    it 'enters the machine at its start, with the compound checkpoint' do
      view = after(placed, started).machine

      expect(view.value).to eq(M::ThingStarted => M::ThingStarted)
      expect(view.in?(M::ThingStarted)).to be(true)
      expect(view.entered_by(:thing_machine)).to eq(started)
    end

    it "routes the machine's commands and the compound checkpoint's own" do
      instance = after(placed, started)
      expect(instance.handles_command?(msg(M::Cancel))).to be(true)

      events = instance.decide(msg(M::Progress, thing_id: 't1'))
      expect(events.map(&:class)).to eq([M::ThingProgressed])
      expect(instance.machine.value).to eq(M::ThingStarted => M::ThingProgressed)
    end

    it 'runs both entries and timers when entered, outermost first' do
      dispatched = after(placed, started).__react(started)

      expect(dispatched.map(&:class)).to eq([M::NotifyCompound, M::NotifyThingStarted, M::Nudge])
      expect(dispatched.last.created_at).to eq(started.created_at + 60)
    end

    it 'exits innermost first when the compound checkpoint is left' do
      dispatched = after(placed, started, progressed, cancelled).__react(cancelled)
      expect(dispatched.map(&:class)).to eq([M::Cleanup, M::NotifyLeft])
    end

    it 'restarts the machine when the compound checkpoint is re-entered' do
      again = msg(M::ThingStarted, thing_id: 't2')
      expect(after(placed, started, progressed, again).machine.value).to eq(M::ThingStarted => M::ThingStarted)
    end

    it "maps the machine's final onto ours" do
      events = after(placed, started, progressed).decide(msg(M::Finish, thing_id: 't1'))
      expect(events.map(&:class)).to eq([M::ThingFinished, M::OrderReady])
    end

    it "keeps the start checkpoint's timer valid while the compound checkpoint is active" do
      nudge = after(placed, started).__react(started).last

      expect(after(placed, started).handles_command?(nudge)).to be(true)
      expect(after(placed, started, cancelled).handles_command?(nudge)).to be(false)
    end
  end

  describe 'rendering' do
    let(:machine) { M::OrderMachine }

    it 'describes the compound checkpoint in to_h, with its start as its initial state' do
      compound = machine.to_h[:states].values.find { |s| s[:class] == M::ThingStarted.name }

      expect(compound[:type]).to eq('compound')
      expect(compound[:states].keys).to include('ThingProgressed', 'ThingFinished')
      expect(compound[:initial]).to eq(compound[:states].values.find { |s| s[:class] == M::ThingStarted.name }[:id])
      expect(compound[:initial]).not_to eq(compound[:id])
    end

    it "gives an instance's value with ids" do
      value = after(placed, started, progressed).to_h[:status][:value]
      expect(value).to eq(M::ThingStarted.type => M::ThingProgressed.type)
    end

    it 'draws the compound checkpoint as a nested state' do
      mermaid = machine.to_mermaid
      expect(mermaid).to include('state "ThingStarted" as MachineCompoundTest_ThingStarted {')
      expect(mermaid).to include('[*] --> MachineCompoundTest_ThingStarted__start')
      expect(mermaid).to include('MachineCompoundTest_ThingStarted__start --> MachineCompoundTest_ThingProgressed : Progress')
    end

    it 'builds a topology' do
      nodes = Sourced::Topology.build([machine])
      ids = nodes.map(&:id)
      expect(ids.uniq.size).to eq(ids.size)
      expect(nodes.select { |n| n.type == 'command' }.map(&:name)).to include(M::Progress.name, M::Cancel.name)
    end
  end

  describe 'the same machine as a child' do
    it 'is invoked with its own partition' do
      instance = M::InvokingMachine.new(order_id: 'o1')
      events = instance.decide(msg(M::PlaceOrder))
      expect(events.map(&:class)).to eq([M::OrderPlaced, M::ThingStarted])
      expect(events.last.payload.thing_id).to eq('t9')
    end
  end

  describe 'definition checks' do
    def machine(&block)
      Class.new(Sourced::Machine) do
        partition_by :order_id
        instance_eval(&block)
      end
    end

    it "rejects a machine that doesn't start at the checkpoint, or has no start" do
      expect { machine { checkpoint M::OrderPlaced, M::ThingMachine } }
        .to raise_error(Sourced::Machine::DefinitionError, /must start at it, its start is .*ThingStarted/)

      no_start = Class.new(Sourced::Machine) { final M::ThingFinished }
      expect { machine { checkpoint no_start } }.to raise_error(Sourced::Machine::DefinitionError, /needs a machine with start/)
    end

    it 'rejects regions in a compound checkpoint, and on_done without a machine' do
      expect { machine { checkpoint(M::ThingMachine) { region(:x) { initial } } } }
        .to raise_error(Sourced::Machine::DefinitionError, /compound checkpoint, so it can't have regions/)
      expect { machine { checkpoint M::OrderPlaced, on_done: { M::ThingFinished => M::OrderReady } } }
        .to raise_error(Sourced::Machine::DefinitionError, /on_done is for a compound checkpoint/)
    end
  end
end
