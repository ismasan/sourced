# frozen_string_literal: true

require 'spec_helper'
require_relative '../support/machine_helpers'

module MachineHistoryTest
  def self.command(name, *attrs) = Sourced::Command.define("machine_history_test.#{name}") { ([:doc_id] + attrs).each { |a| attribute a, String } }

  Start = command(:start)
  Draft = command(:draft)
  Review = command(:review)
  Approve = command(:approve)
  Layout = command(:layout)
  Block = command(:block)
  Unblock = command(:unblock)
  UnblockDeep = command(:unblock_deep)
  Restart = command(:restart)
  Refresh = command(:refresh)
  Resume = command(:resume, :to)
  Bounce = command(:bounce)
  Archive = command(:archive)
  NotifyLaidOut = command(:notify_laid_out)
  Unblocked = Sourced::Event.define('machine_history_test.unblocked') { attribute :doc_id, String }

  class DocMachine < Sourced::Machine
    partition_by :doc_id

    Editing = Checkpoint(:editing)
    Blocked = Checkpoint(:blocked)
    Archived = Checkpoint(:archived)

    initial { command Start => Editing }

    checkpoint Editing do
      region :text do
        initial { command Draft => :drafted }
        checkpoint :drafted do
          region :review do
            initial { command Review => :reviewing }
            checkpoint(:reviewing) { command Approve => :approved }
            checkpoint :approved
          end
        end
      end
      region :layout do
        initial { command Layout => :laid_out }
        checkpoint(:laid_out) { entry { |_doc, _evt| dispatch NotifyLaidOut } }
      end
      command Block => Blocked
      command Refresh => Editing.history(deep: true)
    end

    checkpoint Blocked do
      command Unblock => Editing.history
      command UnblockDeep => Editing.history(deep: true)
      command Restart => Editing
      command Resume => Editing.history | Archived do |_doc, cmd|
        to(cmd.payload.to == 'archive' ? Archived : Editing.history)
      end
      command Bounce => :bouncing
      on Unblocked => Editing.history
    end

    checkpoint(:bouncing) { always Editing.history }

    final Archived
  end

  Write = command(:write)
  Pause = command(:pause)
  Continue = command(:continue)

  # Mounted as a compound checkpoint.
  class DraftMachine < Sourced::Machine
    Writing = Checkpoint(:writing)
    Reviewing = Checkpoint(:reviewing)

    start(Writing) { command Review => Reviewing }
    checkpoint(Reviewing) { command Write => Writing }
  end

  class ProjectMachine < Sourced::Machine
    partition_by :doc_id

    Working = Checkpoint(:working)
    Paused = Checkpoint(:paused)

    initial { command Start => Working }
    checkpoint(Working, DraftMachine) { command Pause => Paused }
    checkpoint(Paused) { command Continue => Working.history }
  end
end

RSpec.describe 'Sourced::Machine history' do
  include MachineHelpers

  MH = MachineHistoryTest

  def msg(klass, **attrs) = klass.new(payload: { doc_id: 'd1', **attrs })
  def decided(machine, *commands) = machine.new(doc_id: 'd1').tap { |m| commands.each { |cmd| m.decide(cmd) } }
  def summary(events) = events.map { |e| [e.class, e.payload.to_h] }
  def moves(machine, *transitions) = summary(transitions.map { |trigger, opts| moved(machine, trigger, **opts, doc_id: 'd1') })
  def cmds(*classes) = classes.map { |klass| msg(klass) }

  let(:doc) { MH::DocMachine }
  let(:edited) { cmds(MH::Start, MH::Draft, MH::Review, MH::Layout, MH::Block) }

  describe 'shallow' do
    it "re-enters each region's last checkpoint, with default entry below it" do
      instance = decided(doc, *edited)
      events = instance.decide(msg(MH::Unblock))

      expect(summary(events)).to eq(moves(
        doc,
        ['machine_history_test.unblock', {
          from: 'blocked', to: 'editing', exited: %w[blocked],
          entered: %w[editing editing.text.drafted editing.layout.laid_out]
        }]
      ))
      # Shallow: drafted's own region starts afresh.
      expect(instance.machine.in?('editing.text.drafted.review.initial')).to be(true)
    end

    it 'enters by default entry where a region has no history' do
      events = decided(doc, *cmds(MH::Start, MH::Layout, MH::Block)).decide(msg(MH::Unblock))
      expect(events.last.payload.entered).to eq(%w[editing editing.layout.laid_out])

      events = decided(doc, *cmds(MH::Start, MH::Block)).decide(msg(MH::Unblock))
      expect(events.last.payload.entered).to eq(%w[editing])
    end

    it 'forgets a region left at its initial position' do
      instance = decided(doc, *edited, msg(MH::Restart), msg(MH::Block))
      expect(instance.decide(msg(MH::Unblock)).last.payload.entered).to eq(%w[editing])
    end

    it 'runs entry actions for the restored checkpoints' do
      unblocked = decided(doc, *edited).decide(msg(MH::Unblock)).last
      expect(decided(doc, *edited).tap { |m| m.evolve([unblocked]) }.__react(unblocked).map(&:class)).to eq([MH::NotifyLaidOut])
    end
  end

  describe 'deep' do
    it 'restores the whole configuration below the checkpoint' do
      events = decided(doc, *edited).decide(msg(MH::UnblockDeep))

      expect(events.last.payload.entered).to eq(
        %w[editing editing.text.drafted editing.text.drafted.review.reviewing editing.layout.laid_out]
      )
    end

    it 're-enters its own checkpoint as it was, as a self-transition' do
      instance = decided(doc, *cmds(MH::Start, MH::Draft, MH::Review, MH::Layout))
      events = instance.decide(msg(MH::Refresh))

      expect(summary(events)).to eq(moves(
        doc,
        ['machine_history_test.refresh', {
          from: 'editing', to: 'editing',
          exited: %w[editing.text.drafted.review.reviewing editing.text.drafted editing.layout.laid_out editing],
          entered: %w[editing editing.text.drafted editing.text.drafted.review.reviewing editing.layout.laid_out]
        }]
      ))
    end
  end

  it 'rebuilds history from stored events on replay' do
    live = doc.new(doc_id: 'd1')
    stored = edited.flat_map { |cmd| live.decide(cmd) }

    replayed = doc.new(doc_id: 'd1').tap { |m| m.evolve(stored) }
    expect(replayed.decide(msg(MH::UnblockDeep)).last.payload.entered)
      .to eq(%w[editing editing.text.drafted editing.text.drafted.review.reviewing editing.layout.laid_out])
  end

  it 'targets history from unions with to, from always, and from on' do
    resumed = decided(doc, *edited).decide(msg(MH::Resume, to: 'editing'))
    expect(resumed.last.payload.entered).to eq(%w[editing editing.text.drafted editing.layout.laid_out])
    expect(decided(doc, *edited).decide(msg(MH::Resume, to: 'archive')).last.class).to eq(doc::Archived)

    bounced = decided(doc, *edited).decide(msg(MH::Bounce))
    expect(bounced.map { |e| [e.payload.trigger, e.payload.target] }).to eq([
      ['machine_history_test.bounce', 'bouncing'], %w[always editing]
    ])
    expect(bounced.last.payload.entered).to eq(%w[editing editing.text.drafted editing.layout.laid_out])

    instance = decided(doc, *edited)
    unblocked = msg(MH::Unblocked)
    instance.evolve([unblocked])
    expect(instance.__decide_on(unblocked).last.payload.entered).to eq(%w[editing editing.text.drafted editing.layout.laid_out])
  end

  it "restores a compound checkpoint's machine where it was" do
    project = MH::ProjectMachine
    instance = decided(project, *cmds(MH::Start, MH::Review, MH::Pause))
    events = instance.decide(msg(MH::Continue))

    expect(events.last.payload.entered).to eq(%w[working working.reviewing])
    expect(instance.machine.value).to eq(project::Working => MH::DraftMachine::Reviewing)
  end

  it "rejects the history of a checkpoint without regions" do
    machine = Class.new(Sourced::Machine) do
      def self.name = 'MachineHistoryTest::NoRegions'
      partition_by :doc_id
      plain = Checkpoint(:plain)
      initial { command MH::Start => plain }
      checkpoint(plain) { command MH::Refresh => plain.history }
    end

    expect { machine.validate! }.to raise_error(Sourced::Machine::DefinitionError, /targets .*plain\.history, but plain has no regions to remember/i)
  end

  it "rejects a done that targets its own checkpoint's history" do
    machine = Class.new(Sourced::Machine) do
      def self.name = 'MachineHistoryTest::SelfJoin'
      partition_by :doc_id
      busy = Checkpoint(:busy)
      initial { command MH::Start => busy }
      checkpoint(busy) do
        region(:r) { initial { command MH::Refresh => :done_r }; final :done_r }
        done busy.history
      end
    end

    expect { machine.validate! }.to raise_error(Sourced::Machine::DefinitionError, /would restore its final regions and join again/)
  end

  describe 'rendering' do
    it 'adds history nodes to to_h where something targets them' do
      chart = doc.to_h
      editing = chart[:states]['editing']

      expect(editing[:states]['$history']).to eq(id: 'editing.$history', key: '$history', type: 'history', history: 'shallow')
      expect(editing[:states]['$deep_history']).to eq(id: 'editing.$deep_history', key: '$deep_history', type: 'history', history: 'deep')
      expect(chart[:states]['blocked'][:on]['machine_history_test.unblock'][:targets]).to eq(['editing.$history'])
      expect(chart[:states]['blocked'][:on]['machine_history_test.resume'][:targets]).to eq(%w[editing.$history archived])
      expect(chart[:states]['bouncing'][:always]).to eq([{ target: 'editing.$history' }])
      expect(MH::ProjectMachine.to_h[:states]['working'][:states].keys).to eq(%w[writing reviewing $history])
      expect(MH::ProjectMachine.to_h[:states]['paused'][:on]['machine_history_test.continue'][:targets]).to eq(['working.$history'])
    end

    it 'labels history edges in Mermaid' do
      diagram = doc.to_mermaid
      expect(diagram).to include('blocked --> editing : Unblock (H)')
      expect(diagram).to include('blocked --> editing : UnblockDeep (H*)')
      expect(diagram).to include('bouncing --> editing : always (H)')
    end

    it "produces the checkpoint's class in Topology" do
      nodes = Sourced::Topology.build([doc])
      unblock = nodes.find { |n| n.type == 'command' && n.id == 'machine_history_test.unblock' }
      expect(unblock.produces).to eq(['machine_history_test.doc_machine.runtime.editing'])
    end
  end
end
