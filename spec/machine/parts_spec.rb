# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachinePartsTest
  class Payroll < Sourced::Machine
    partition_by :payroll_id, run: Sourced::Types::Integer

    Start = Command(:start)

    Pending = State(:pending)
    Done = State(:done)
    Failed = State(:failed)

    context { { log: [] } }

    class Providers
      class CheckMachine < Sourced::Machine::Part
        part_of Payroll

        Accept = Command(:accept)
        Checked = State(:checked)

        initial { command Accept => Checked }
        final Checked
      end
    end

    class PendingMachine < Sourced::Machine::Part
      part_of Payroll

      Accept = Command(:accept) { attribute :by, String }
      Reject = Command(:reject)
      Accepted = Event(:accepted) { attribute :by, String }
      Unread = Event(:unread)

      Queued = State(:queued)
      Approved = State(:approved)
      Rejected = State(:rejected)

      evolve(Accepted) { |context, evt| context[:log] << evt.payload.by }

      started Queued do
        command Accept => Approved do |_context, cmd|
          event Accepted, by: cmd.payload.by
        end
        command Reject => Rejected
        region :check, Providers::CheckMachine
      end

      final Approved
      final Rejected
    end

    class AuditMachine < Sourced::Machine::Part
      part_of Payroll
      part_prefix 'audit.trail'

      Note = Command(:note)
      Noted = State(:noted)

      initial { command Note => Noted }
      final Noted
    end

    class UnsettledMachine < Sourced::Machine::Part
      part_of Payroll

      Waiting = State(:waiting)

      started(Waiting) {}
    end

    # Part of a part: Payroll's, nested in UnsettledMachine's prefix.
    class OpenMachine < Sourced::Machine::Part
      part_of UnsettledMachine

      Settle = Command(:settle)
    end

    initial { command Start => Pending }

    state Pending, PendingMachine do
      on_final PendingMachine::Approved => Done, PendingMachine::Rejected => Failed
    end

    final Done
    final Failed
  end

  class Other < Sourced::Machine
    partition_by :payroll_id
  end

  # A plain machine, which can't be mounted.
  class Standalone < Sourced::Machine
    partition_by :payroll_id

    started(:ok) {}
  end
end

RSpec.describe Sourced::Machine::Part do
  let(:mod) { MachinePartsTest }
  let(:payroll) { mod::Payroll }
  let(:pending) { payroll::PendingMachine }
  let(:check) { payroll::Providers::CheckMachine }

  def part(owner = payroll, &block)
    Class.new(described_class) do
      part_of owner
      part_prefix 'anon'
      class_exec(&block) if block
    end
  end

  describe 'declaring a part' do
    it 'is a part of its owner, and not a machine of its own' do
      expect(pending.owner).to be(payroll)
      expect(pending < described_class).to be(true)
      expect(payroll < described_class).to be_nil
    end

    it "is bound to a part's owner when it's part_of that part" do
      expect(payroll::OpenMachine.owner).to be(payroll)
    end

    it 'takes part_of first' do
      expect { Class.new(described_class) { State(:x) } }.to raise_error(Sourced::Machine::DefinitionError, /declare part_of first/)
      expect { Class.new(described_class) { initial {} } }.to raise_error(Sourced::Machine::DefinitionError, /declare part_of first/)
      expect { Class.new(described_class) { Command(:x) } }.to raise_error(Sourced::Machine::DefinitionError, /declare part_of first/)

      owner = payroll
      expect { Class.new(described_class) { part_prefix 'x'; State(:a); part_of owner } }
        .to raise_error(Sourced::Machine::DefinitionError, /declare part_of first/)
    end

    it 'raises on part_of something that is not a machine, or twice' do
      expect { Class.new(described_class) { part_of String } }.to raise_error(Sourced::Machine::DefinitionError, /takes a Sourced::Machine/)
      expect { part { part_of MachinePartsTest::Payroll } }.to raise_error(Sourced::Machine::DefinitionError, /declared twice/)
      unbound = Class.new(described_class)
      expect { Class.new(described_class) { part_of unbound } }.to raise_error(Sourced::Machine::DefinitionError, /part of nothing yet/)
    end

    it 'needs a part_prefix when anonymous' do
      owner = payroll
      expect { Class.new(described_class) { part_of owner; Command(:x) } }
        .to raise_error(Sourced::Machine::DefinitionError, /anonymous part needs a part_prefix/)
    end
  end

  describe 'messages' do
    it "are typed under the owner's prefix and the part's" do
      expect(pending::Accept.type).to eq('machine_parts_test.payroll.pending.accept')
      expect(pending::Accepted.type).to eq('machine_parts_test.payroll.pending.accepted')
      expect(check::Accept.type).to eq('machine_parts_test.payroll.providers.check.accept')
    end

    it "declare the owner's partition keys first, typed by its schema" do
      expect(pending::Accept.payload_attribute_names).to eq(%i[payroll_id run by])
      expect(pending::Reject.payload_attribute_names).to eq(%i[payroll_id run])
      expect(pending::Accept.new(payload: { payroll_id: 'p1', run: '1', by: 'x' })).not_to be_valid
      expect(pending::Accept.new(payload: { payroll_id: 'p1', run: 1, by: 'x' })).to be_valid
    end

    it 'take a declared part_prefix' do
      expect(payroll::AuditMachine.part_prefix).to eq('audit.trail')
      expect(payroll::AuditMachine::Note.type).to eq('machine_parts_test.payroll.audit.trail.note')
    end

    it "don't collide across parts defining the same name" do
      expect(pending::Accept).not_to be(check::Accept)
      expect(Sourced::Message.registry[pending::Accept.type]).to be(pending::Accept)
      expect(Sourced::Message.registry[check::Accept.type]).to be(check::Accept)
      expect(payroll.handled_commands).to include(pending::Accept, check::Accept)
    end

    it "nest a part of a part under that part's prefix" do
      expect(payroll::OpenMachine.part_prefix).to eq('unsettled.open')
      expect(payroll::OpenMachine::Settle.type).to eq('machine_parts_test.payroll.unsettled.open.settle')
      expect(payroll::OpenMachine::Settle.payload_attribute_names).to eq(%i[payroll_id run])
    end

    it 'take part_prefix only before states and messages' do
      expect { part { State(:a); part_prefix 'b' } }.to raise_error(Sourced::Machine::DefinitionError, /before states and messages/)
    end
  end

  describe 'states' do
    it "are typed under the owner's runtime prefix, with the owner's keys" do
      expect(pending::Queued.type).to eq('machine_parts_test.payroll.runtime.pending.queued')
      expect(pending::Queued.payload_attribute_names.first(2)).to eq(%i[payroll_id run])
    end

    it "are the owner's state classes, declared by the part" do
      expect(pending::Queued.machine).to be(pending)
      expect(payroll.transition_classes).to include(pending::Queued, pending::Approved, check::Checked)
      expect(payroll.handled_messages_for_evolve).to include(pending::Queued, check::Checked)
    end

    it "are shared by a part's placements, told apart by their paths" do
      owner = Class.new(Sourced::Machine) do
        partition_by :payroll_id, run: Sourced::Types::Integer
        consumer_group 'parts_twice_owner'
      end
      twice = Class.new(described_class) do
        part_of owner
        part_prefix 'twice'
        initial { command MachinePartsTest::Payroll::Start => :done }
        final :done
      end
      owner.state(:one) do
        region :left, twice
        region :right, twice
      end
      owner.validate!

      expect(owner.tree.placements(twice::Done).map(&:path)).to eq(%w[one.left.done one.right.done])
      expect(owner.transition_classes.count(twice::Done)).to eq(1)
    end
  end

  describe 'what a part has none of' do
    it "isn't a machine" do
      expect(pending < Sourced::Machine).to be_nil
      %i[partition_by consumer_group context before_evolve reaction sync after_sync stopped validate! handle_claim].each do |dsl|
        expect(pending).not_to respond_to(dsl)
      end
      expect { pending.message_prefix('x') }.to raise_error(ArgumentError)
    end

    it "can't be instantiated" do
      expect { pending.new }.to raise_error(NoMethodError, /private method 'new'/)
    end

    it "can't be registered" do
      expect { Sourced::Router.new(store: Sourced::Store.new(Sequel.sqlite), reactors: [pending]) }
        .to raise_error(ArgumentError, /PendingMachine isn't a reactor: .*Missing methods: handled_messages, handle_claim/)
      expect { Sourced::Config.register(Sourced::Config.build, pending) }
        .to raise_error(ArgumentError, /PendingMachine isn't a reactor/)
    end

    it "can't be a child" do
      parent = Class.new(Sourced::Machine) { partition_by :payroll_id }
      expect { parent.initial { command MachinePartsTest::Payroll::Start, starts: MachinePartsTest::Payroll::PendingMachine } }
        .to raise_error(Sourced::Machine::DefinitionError, /part of .*Payroll, not a machine to start/)
    end
  end

  describe 'evolve' do
    it "evolves the owner's context" do
      expect(payroll.handled_messages_for_evolve).to include(pending::Accepted)
    end

    it "adds to the owner's evolve types only the events it evolves" do
      expect(payroll.handled_messages_for_evolve).not_to include(pending::Unread)
    end

    it 'needs a block' do
      expect { part { evolve MachinePartsTest::Payroll::PendingMachine::Accepted } }
        .to raise_error(Sourced::Machine::DefinitionError, /needs a block/)
    end

    it 'raises once its owner is validated' do
      payroll.validate!
      expect { pending.evolve(pending::Accepted) {} }.to raise_error(Sourced::Machine::DefinitionError, /after .*Payroll was validated/)
    end

    def evolving_part(owner, prefix, evt)
      Class.new(described_class) do
        part_of owner
        part_prefix prefix
        evolve(evt) { |_context, _evt| nil }
        started(:a) {}
      end
    end

    let(:owner) do
      Class.new(Sourced::Machine) do
        partition_by :payroll_id
        consumer_group 'parts_evolve_owner'
      end
    end
    let(:evt) { mod::Payroll::PendingMachine::Accepted }

    it 'raises when its owner evolves the same event' do
      owner.evolve(evt) { |_context, _evt| nil }
      owner.state(:one, evolving_part(owner, 'one', evt))

      expect { owner.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Accepted is evolved by both #{Regexp.escape(owner.to_s)} and /)
    end

    it 'raises when another part evolves the same event' do
      owner.state(:one, evolving_part(owner, 'one', evt))
      owner.state(:two, evolving_part(owner, 'two', evt))

      expect { owner.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Accepted is evolved by both/)
    end
  end

  describe 'mounting' do
    it "puts its commands among the owner's, through the tree" do
      expect(payroll.handled_commands).to include(pending::Accept, pending::Reject, check::Accept)
      expect(payroll.tree.mounted_machines).to eq([pending, check])
    end

    it 'mounts a part in a part of the same owner' do
      expect(pending.mounts.values.map(&:machine)).to eq([check])

      inner = part { initial { command MachinePartsTest::Payroll::Start => :x }; final :x }
      expect { part { started(:a) { region :inner, inner } } }.not_to raise_error
    end

    it 'raises under a machine other than its owner' do
      expect { Class.new(mod::Other) { consumer_group 'other_x'; state(:x) { region :check, MachinePartsTest::Payroll::Providers::CheckMachine } } }
        .to raise_error(Sourced::Machine::DefinitionError, /CheckMachine.*part of .*Payroll, so it mounts only in/)
    end

    it 'raises under a part of another owner' do
      foreign = Class.new(described_class) do
        part_of MachinePartsTest::Other
        part_prefix 'foreign'
      end
      expect { foreign.started(:a) { region :check, MachinePartsTest::Payroll::Providers::CheckMachine } }
        .to raise_error(Sourced::Machine::DefinitionError, /mounts only in/)
    end

    it 'raises on a machine, pointing to parts and children' do
      mounted = mod::Standalone
      expect { Class.new(mod::Other) { consumer_group 'other_x'; state(:x) { region :standalone, mounted } } }
        .to raise_error(
          Sourced::Machine::DefinitionError,
          /region :standalone: can't mount MachinePartsTest::Standalone, a Sourced::Machine: .*Sourced::Machine::Part \(part_of .*\).*child \(start, starts:\)/
        )
      expect { Class.new(mod::Other) { consumer_group 'other_y'; state(:x, mounted) } }
        .to raise_error(Sourced::Machine::DefinitionError, /state .*X: can't mount MachinePartsTest::Standalone, a Sourced::Machine/)
    end

    it "raises on anything that isn't a part" do
      expect { Class.new(mod::Other) { consumer_group 'other_z'; state(:x) { region :thing, String } } }
        .to raise_error(Sourced::Machine::DefinitionError, /String isn't a Sourced::Machine::Part/)
    end
  end

  describe 'running in its owner' do
    let(:store) { Sourced::Store.new(Sequel.sqlite).tap(&:install!) }
    let(:router) { Sourced::Router.new(store:, reactors: [payroll]).setup! }
    let(:keys) { { payroll_id: 'p1', run: 1 } }

    before do
      router
      Sourced.handle!(payroll, payroll::Start.new(payload: keys), store:)
    end

    it "decides a part's command with the owner's keys in its payload" do
      result = Sourced.handle!(payroll, pending::Accept.new(payload: { **keys, by: 'joe' }), store:)

      expect(result.events.map(&:class)).to eq([pending::Accepted, pending::Approved, payroll::Done])
      expect(pending::Approved === result.events[1]).to be(true)
      expect(result.events[1].type).to eq('machine_parts_test.payroll.runtime.pending.approved')
      expect(payroll.load(store:, **keys).value).to eq(payroll::Done)
    end

    it 'decides commands of two parts with the same name apart' do
      Sourced.handle!(payroll, check::Accept.new(payload: keys), store:)

      expect(payroll.load(store:, **keys).value).to eq(payroll::Pending => { pending::Queued => { check: check::Checked } })
    end

    it 'replays from the log through Sourced.load' do
      Sourced.handle!(payroll, check::Accept.new(payload: keys), store:)
      Sourced.handle!(payroll, pending::Accept.new(payload: { **keys, by: 'ann' }), store:)
      router.drain

      instance, = Sourced.load(payroll, store:, **keys)
      expect(instance.machine.value).to eq(payroll::Done)
      expect(instance.context[:log]).to eq(['ann'])
      expect(payroll.load(store:, payroll_id: 'p2', run: 1).value).to eq(:initial)
    end
  end
end
