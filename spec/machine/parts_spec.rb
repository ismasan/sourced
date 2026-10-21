# frozen_string_literal: true

require 'spec_helper'
require 'sequel'

module MachinePartsTest
  class Payroll < Sourced::Machine
    partition_by :payroll_id, run: Sourced::Types::Integer

    Start = Command(:start)

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
      part_of Payroll, prefix: 'audit.trail'

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

    initial { command Start => PendingMachine }

    state PendingMachine do
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
      part_of owner, prefix: 'anon'
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
      expect { Class.new(described_class) { State(:a); part_of owner, prefix: 'x' } }
        .to raise_error(Sourced::Machine::DefinitionError, /declare part_of first/)
    end

    it 'raises on part_of something that is not a machine, or twice' do
      expect { Class.new(described_class) { part_of String } }.to raise_error(Sourced::Machine::DefinitionError, /takes a Sourced::Machine/)
      expect { part { part_of MachinePartsTest::Payroll } }.to raise_error(Sourced::Machine::DefinitionError, /declared twice/)
      unbound = Class.new(described_class)
      expect { Class.new(described_class) { part_of unbound } }.to raise_error(Sourced::Machine::DefinitionError, /part of nothing yet/)
    end

    it "needs part_of's prefix: when anonymous, and not a blank one" do
      owner = payroll
      expect { Class.new(described_class) { part_of owner; Command(:x) } }
        .to raise_error(Sourced::Machine::DefinitionError, /anonymous part needs part_of's prefix:/)
      expect { Class.new(described_class) { part_of owner, prefix: ' ' } }
        .to raise_error(Sourced::Machine::DefinitionError, /prefix: can't be blank/)
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
        part_of owner, prefix: 'twice'
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
        part_of owner, prefix: prefix
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
      owner.state(evolving_part(owner, 'one', evt))

      expect { owner.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Accepted is evolved by both #{Regexp.escape(owner.to_s)} and /)
    end

    it 'raises when another part evolves the same event' do
      owner.state(evolving_part(owner, 'one', evt))
      owner.state(evolving_part(owner, 'two', evt))

      expect { owner.validate! }.to raise_error(Sourced::Machine::DefinitionError, /Accepted is evolved by both/)
    end
  end

  describe 'mounting' do
    it 'raises when two parts of one owner share a prefix' do
      owner = Class.new(Sourced::Machine) do
        partition_by :payroll_id
        consumer_group 'parts_prefix_owner'
      end
      two = Array.new(2) do
        Class.new(described_class) do
          part_of owner, prefix: 'review'
          started(:a) {}
        end
      end
      owner.state(:one) { region :left, two[0] }
      owner.state(:two) { region :right, two[1] }

      expect { owner.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /share the part prefix "review", so their messages and states would have the same types/)
    end

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
        part_of MachinePartsTest::Other, prefix: 'foreign'
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
      expect { Class.new(mod::Other) { consumer_group 'other_y'; state(mounted) } }
        .to raise_error(Sourced::Machine::DefinitionError, /state MachinePartsTest::Standalone: can't mount MachinePartsTest::Standalone, a Sourced::Machine/)
    end

    it "raises on anything that isn't a part" do
      expect { Class.new(mod::Other) { consumer_group 'other_z'; state(:x) { region :thing, String } } }
        .to raise_error(Sourced::Machine::DefinitionError, /String isn't a Sourced::Machine::Part/)
    end
  end

  describe 'its compound state' do
    let(:owner) do
      Class.new(Sourced::Machine) do
        partition_by :payroll_id
        consumer_group 'parts_compound_owner'
      end
    end

    def compound_part(owner, prefix)
      Class.new(described_class) do
        part_of owner, prefix: prefix
        started(:open) {}
        final :done
      end
    end

    it "is Part::State, typed under the owner's runtime prefix, with the owner's keys" do
      expect(pending::State).to be < Sourced::Machine::State
      expect(pending.to_state_handle).to be(pending::State)
      expect(pending::State.type).to eq('machine_parts_test.payroll.runtime.pending')
      expect(pending::State.payload_attribute_names.first(2)).to eq(%i[payroll_id run])
      expect(pending::State.state_name).to eq(:pending)
      expect(pending::State.machine).to be(pending)
      expect(pending::State.part).to be(pending)
      expect(pending::State.state).to be(pending::State)
      expect(pending::State.to_state_handle).to be(pending::State)
      expect(pending::Queued.part).to be_nil
    end

    it "is named after its prefix's last segment, nesting under an enclosing part's type" do
      expect(payroll::OpenMachine::State.type).to eq('machine_parts_test.payroll.runtime.unsettled.open')
      expect(payroll::OpenMachine::State.state_name).to eq(:open)
      expect(payroll::AuditMachine::State.state_name).to eq(:trail)
    end

    it "isn't one of the part's declared states" do
      expect(pending.declared_states.values).not_to include(pending::State)
    end

    it 'is placed by state Part, which mounts the part' do
      node = payroll.root.nodes.fetch(pending::State)
      mount = node.regions.first

      expect(payroll.nodes[pending::State]).to be(node)
      expect(mount.machine).to be(pending)
      expect(mount).to be_compound
      expect(payroll.tree.placements(pending::State).map(&:path)).to eq(%w[pending])
      expect(payroll.transition_classes).to include(pending::State)
    end

    it "isn't a transition class of an owner mounting the part only as a region" do
      expect(payroll.transition_classes).not_to include(check::State)
    end

    it 'stands in for its state as a target: command, union, nil, array, to, always, on_final, done' do
      review = compound_part(owner, 'review')
      other = compound_part(owner, 'other')
      go = owner.Command(:go)
      choose = owner.Command(:choose)
      skip = owner.Command(:skip)
      owner.initial do
        command go => review
        command(choose => review | nil) { |_context, _cmd| to(review) }
        command(skip => [other, nil]) { |_context, _cmd| to(other) }
      end
      owner.state(other) do
        on_final other::Done => review
        command(choose => other | review) { |_context, _cmd| to(review) }
      end
      owner.state(review) { on_final review::Done => :routing }
      owner.state(:routing) { always review }
      owner.validate!

      initial = owner.root.initial_node
      expect(initial.commands[go].targets).to eq([review::State])
      expect(initial.commands[choose].targets).to eq([review::State, nil])
      expect(initial.commands[skip].targets).to eq([other::State, nil])
      expect(owner.nodes[other::State].commands[choose].targets).to eq([other::State, review::State])
      expect(owner.mounts[:other].on_final).to eq(other::Done => review::State)
      expect(owner.nodes[owner::Routing].always.map(&:target)).to eq([review::State])

      instance = owner.new(payroll_id: 'p1')
      events = instance.decide(choose.new(payload: { payroll_id: 'p1' }))
      expect(events.map(&:class)).to eq([review::State])
      expect(events.last.payload.entered).to eq(%w[review review.open])
    end

    it 'takes anything responding to to_state_handle as a target' do
      review = compound_part(owner, 'review')
      handle = Object.new
      handle.define_singleton_method(:to_state_handle) { review::State }
      go = owner.Command(:go)
      owner.initial { command go => handle }
      owner.state(review)

      expect(owner.root.initial_node.commands[go].targets).to eq([review::State])
      nope = owner.Command(:nope)
      expect { owner.state(:z) { command nope => Object.new } }
        .to raise_error(NoMethodError, /to_state_handle/)
    end

    it "has the part's history stand for its compound state's" do
      expect(pending.history).to eq(pending::State.history)
      expect(pending.history(deep: true)).to eq(pending::State.history(deep: true))
    end

    it "raises on the two-argument form, pointing to state Part" do
      review = compound_part(owner, 'review')
      expect { owner.state(:x, review) }
        .to raise_error(Sourced::Machine::DefinitionError, /a state takes no part; a part is its own compound state, so mount it with state /)
      expect { owner.started(:x, review) }.to raise_error(Sourced::Machine::DefinitionError, /mount it with state /)
      expect { owner.state(:y) { region(:r) { state :x, review } } }.to raise_error(Sourced::Machine::DefinitionError, /mount it with state /)
    end

    it "can't be a final state" do
      review = compound_part(owner, 'review')
      expect { owner.final(review) }.to raise_error(Sourced::Machine::DefinitionError, /a final state can't be compound/)
      expect { owner.final(review::State) }.to raise_error(Sourced::Machine::DefinitionError, /a final state can't be compound/)
    end

    it "raises when the owner's own state has a part's type, whichever comes first" do
      clashing = stub_const('MachinePartsTest::ClashingOwner', Class.new(Sourced::Machine) { partition_by :payroll_id })
      clashing.State(:review)
      expect { compound_part(clashing, 'review') }
        .to raise_error(Sourced::Machine::DefinitionError, /message type "machine_parts_test.clashing_owner.runtime.review" is already MachinePartsTest::ClashingOwner:review's/)

      audit = compound_part(clashing, 'audit')
      stub_const('MachinePartsTest::ClashingOwner::AuditPart', audit)
      expect { clashing.State(:audit) }
        .to raise_error(Sourced::Machine::DefinitionError, /message type "machine_parts_test.clashing_owner.runtime.audit" is already MachinePartsTest::ClashingOwner::AuditPart::State's/)
    end

    it 'raises when two parts placed by one machine have compound states of the same name' do
      one = compound_part(owner, 'review')
      two = compound_part(owner, 'audit.review')
      owner.state(one)
      expect { owner.state(:x) { region(:r) { state two } } }
        .to raise_error(Sourced::Machine::DefinitionError, /compound state is named :review too/)
    end

    it 'is shared by placements in two regions, told apart by paths' do
      review = compound_part(owner, 'review')
      mounter = owner
      wrapper = Class.new(described_class) do
        part_of mounter, prefix: 'wrapper'
        started(review) {}
      end
      owner.state(:both) do
        region :left, wrapper
        region :right, wrapper
      end
      owner.validate!

      expect(owner.tree.placements(review::State).map(&:path)).to eq(%w[both.left.review both.right.review])
      expect(owner.transition_classes.count(review::State)).to eq(1)
    end
  end

  describe 'as a handler declaration' do
    let(:keys) { { payroll_id: 'p1', run: 1 } }

    def decided
      instance = payroll.new(keys)
      instance.decide(payroll::Start.new(payload: keys)) +
        instance.decide(pending::Accept.new(payload: { **keys, by: 'joe' }))
    end

    it 'resolves to Part::State through to_message_class' do
      expect(pending.to_message_class).to be(pending::State)
    end

    it "evolves only transitions to the part's compound state, not those straight to its sub-states" do
      projection = Class.new(Sourced::Projection) do
        context { [] }
        evolve(MachinePartsTest::Payroll::PendingMachine) { |context, evt| context << evt.payload.entered }
      end
      events = decided

      expect(projection.handled_messages).to eq([pending::State])
      expect(events.map(&:class)).to include(pending::State, pending::Approved)
      expect(projection.new.evolve(events)).to eq([%w[pending pending.queued]])
    end

    it 'subscribes a projector evolving and reacting to a part to Part::State' do
      projector = Class.new(Sourced::Projector::StateStored) do
        partition_by :payroll_id
        consumer_group 'parts_handler_projector'
        evolve(MachinePartsTest::Payroll::PendingMachine) { |context, _evt| context }
        reaction(MachinePartsTest::Payroll::PendingMachine) { |_context, _evt| nil }
      end

      expect(projector.handled_messages).to eq([pending::State])
      expect(projector.handled_messages_for_react).to eq([pending::State])
      expect(projector.new(payroll_id: 'p1').reacts_to?(decided.first)).to be(true)
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

    it "enters a part's compound state at the part's start, appending Part::State" do
      entered = store.read_all(limit: 10).messages.last

      expect(entered).to be_a(pending::State)
      expect(pending::State === entered).to be(true)
      expect(entered.payload.to_h).to include(payroll_id: 'p1', run: 1, target: 'pending', entered: %w[pending pending.queued])
      expect(payroll.load(store:, **keys).value).to eq(pending::State => { pending::Queued => { check: :initial } })
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

      expect(payroll.load(store:, **keys).value).to eq(pending::State => { pending::Queued => { check: check::Checked } })
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
