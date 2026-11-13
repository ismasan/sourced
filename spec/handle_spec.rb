# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'

module HandleTestMessages
  CreateDevice = Sourced::Command.define('handle_test.create_device') do
    attribute :device_id, String
    attribute :name, Sourced::Types::String.present
  end

  DeviceCreated = Sourced::Event.define('handle_test.device_created') do
    attribute :device_id, String
    attribute :name, String
  end

  ActivateDevice = Sourced::Command.define('handle_test.activate_device') do
    attribute :device_id, String
  end

  DeviceActivated = Sourced::Event.define('handle_test.device_activated') do
    attribute :device_id, String
  end

  # Handled by HandleTestReactingDecider.
  RegisterDevice = Sourced::Command.define('handle_test.register_device') do
    attribute :device_id, String
    attribute :name, String
  end

  NotifyCreated = Sourced::Command.define('handle_test.notify_created') do
    attribute :device_id, String
  end
end

class HandleTestDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'handle_test_decider'

  context { { exists: false, active: false } }

  evolve HandleTestMessages::DeviceCreated do |context, _evt|
    context[:exists] = true
  end

  evolve HandleTestMessages::DeviceActivated do |context, _evt|
    context[:active] = true
  end

  command HandleTestMessages::CreateDevice => HandleTestMessages::DeviceCreated do |context, cmd|
    raise 'Already exists' if context[:exists]
    event HandleTestMessages::DeviceCreated, device_id: cmd.payload.device_id, name: cmd.payload.name
  end

  command HandleTestMessages::ActivateDevice => HandleTestMessages::DeviceActivated do |context, cmd|
    raise 'Not found' unless context[:exists]
    raise 'Already active' if context[:active]
    event HandleTestMessages::DeviceActivated, device_id: cmd.payload.device_id
  end
end

# A reactor PartitionSchema.check! doesn't cover (no partition_schema), so a
# command without its partition key reaches handle!'s runtime check.
module HandleUncheckedMessages
  AddNote = Sourced::Command.define('handle_unchecked_test.add_note') do
    attribute :text, String
  end
end

class HandleUncheckedReactor
  def self.claim_selector = Sourced::Selector[HandleUncheckedMessages::AddNote].by(:device_id)
  def self.handle_claim(claim) = claim.messages.map { |m| [Sourced::Actions::OK, m] }

  def initialize(_values); end
  def decide(_cmd) = []
end

# Events carry order_id in the payload and tenant_id as a tag; the command declares both.
module HandleMixedTestMessages
  AddNote = Sourced::Command.define('handle_mixed_test.add_note') do
    attribute :order_id, String
    attribute :tenant_id, String
    attribute :note, String
  end

  SomethingHappened = Sourced::Event.define('handle_mixed_test.something_happened') do
    attribute :order_id, String
    attribute :note, String
  end

  # Foreign event carrying only order_id.
  OrderTouched = Sourced::Event.define('handle_mixed_test.order_touched') do
    attribute :order_id, String
  end

  NotesCounted = Sourced::Event.define('handle_mixed_test.notes_counted') do
    attribute :order_id, String
    attribute :tenant_id, String
    attribute :count, Integer
  end
end

class HandleMixedTestDecider < Sourced::Decider
  partition_by :order_id, :tenant_id
  consumer_group 'handle_mixed_test_decider'

  context { { notes: [], touched: false } }

  evolve HandleMixedTestMessages::SomethingHappened do |context, evt|
    context[:notes] << evt.payload.note
  end

  evolve HandleMixedTestMessages::OrderTouched do |context, _evt|
    context[:touched] = true
  end

  command HandleMixedTestMessages::AddNote => HandleMixedTestMessages::NotesCounted do |context, cmd|
    event HandleMixedTestMessages::NotesCounted,
          order_id: cmd.payload.order_id, tenant_id: cmd.payload.tenant_id, count: context[:notes].size + 1
  end
end

class HandleTestReactingDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'handle_test_reacting_decider'

  command HandleTestMessages::RegisterDevice => HandleTestMessages::DeviceCreated do |_context, cmd|
    event HandleTestMessages::DeviceCreated, device_id: cmd.payload.device_id, name: cmd.payload.name
  end

  reaction HandleTestMessages::DeviceCreated => HandleTestMessages::NotifyCreated do |_context, evt|
    dispatch HandleTestMessages::NotifyCreated, device_id: evt.payload.device_id
  end
end

# Each transition's entry action dispatches Alert.
class HandleTestDoorMachine < Sourced::Machine
  partition_by :door_id
  consumer_group 'handle_test.door_machine'

  Opened = State(:opened)
  Closed = State(:closed)
  OpenDoor = Command(:open_door)
  CloseDoor = Command(:close_door)
  Alert = Command(:alert)

  initial { command OpenDoor => Opened }

  state Opened do
    entry Alert
    command CloseDoor => Closed
  end

  state Closed do
    entry Alert
    command OpenDoor => Opened
  end
end

# Refuses ActivateDevice until the device exists, instead of raising.
class HandleTestGatedDecider < HandleTestDecider
  consumer_group 'handle_test_gated_decider'

  def handles_command?(command)
    return super unless command.is_a?(HandleTestMessages::ActivateDevice)

    super && context[:exists]
  end
end

RSpec.describe 'Sourced.handle!' do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::Store.new(db) }

  before { store.install! }

  describe 'valid command, no prior history' do
    it 'returns command, reactor, and events' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      result = Sourced.handle!(HandleTestDecider, cmd, store: store)

      expect(result).to be_a(Sourced::HandleResult)
      expect(result.command).to eq(cmd)
      expect(result.reactor).to be_a(HandleTestDecider)
      expect(result.events.size).to eq(1)
      expect(result.events.first).to be_a(HandleTestMessages::DeviceCreated)
    end

    it 'supports array destructuring' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      cmd_out, reactor, events = Sourced.handle!(HandleTestDecider, cmd, store: store)

      expect(cmd_out).to eq(cmd)
      expect(reactor).to be_a(HandleTestDecider)
      expect(events.size).to eq(1)
    end

    it 'evolves reactor context' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      _cmd, reactor, _events = Sourced.handle!(HandleTestDecider, cmd, store: store)

      expect(reactor.context[:exists]).to be true
    end

    it 'appends command and correlated events to the store' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      _cmd, _reactor, events = Sourced.handle!(HandleTestDecider, cmd, store: store)

      # Both command and event should be in the store
      all = store.db[:sourced_messages].order(:position).all
      expect(all.size).to eq(2)
      expect(all[0][:message_type]).to eq('handle_test.create_device')
      expect(all[1][:message_type]).to eq('handle_test.device_created')
    end

    it 'correlates events with the command' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      _cmd, _reactor, events = Sourced.handle!(HandleTestDecider, cmd, store: store)

      expect(events.first.causation_id).to eq(cmd.id)
      expect(events.first.correlation_id).to eq(cmd.correlation_id)
    end
  end

  describe 'valid command with prior history' do
    before do
      # Create device first
      Sourced.handle!(
        HandleTestDecider,
        HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )
    end

    it 'loads history and evolves before deciding' do
      cmd = HandleTestMessages::ActivateDevice.new(
        payload: { device_id: 'd1' }
      )

      _cmd, reactor, events = Sourced.handle!(HandleTestDecider, cmd, store: store)

      expect(events.size).to eq(1)
      expect(events.first).to be_a(HandleTestMessages::DeviceActivated)
      expect(reactor.context[:exists]).to be true
      expect(reactor.context[:active]).to be true
    end
  end

  describe 'invalid command' do
    it 'returns immediately without appending' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 123 } # name should be a present string
      )

      cmd_out, reactor, events = Sourced.handle!(HandleTestDecider, cmd, store: store)

      expect(cmd_out.valid?).to be false
      expect(reactor).to be_a(HandleTestDecider)
      expect(events).to eq([])

      # Nothing appended
      expect(store.db[:sourced_messages].count).to eq(0)
    end
  end

  describe 'domain invariant violation' do
    it 'raises the domain error' do
      cmd = HandleTestMessages::ActivateDevice.new(
        payload: { device_id: 'd1' }
      )

      expect {
        Sourced.handle!(HandleTestDecider, cmd, store: store)
      }.to raise_error(RuntimeError, 'Not found')
    end
  end

  describe 'commands the reactor does not handle' do
    it 'raises CommandNotHandledError for a command class without a handler, appending nothing' do
      cmd = HandleTestMessages::NotifyCreated.new(payload: { device_id: 'd1' })

      expect {
        Sourced.handle!(HandleTestDecider, cmd, store: store)
      }.to raise_error(Sourced::CommandNotHandledError, /HandleTestDecider doesn't handle/)
      expect(store.read_all(limit: 10).messages).to be_empty
    end

    it "raises CommandNotHandledError when the reactor's context refuses the command" do
      cmd = HandleTestMessages::ActivateDevice.new(payload: { device_id: 'd1' })

      expect {
        Sourced.handle!(HandleTestGatedDecider, cmd, store: store)
      }.to raise_error(Sourced::CommandNotHandledError)
      expect(store.read_all(limit: 10).messages).to be_empty
    end

    it 'handles the command once the context allows it' do
      Sourced.handle!(HandleTestGatedDecider, HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }), store: store)
      _cmd, _reactor, events = Sourced.handle!(
        HandleTestGatedDecider, HandleTestMessages::ActivateDevice.new(payload: { device_id: 'd1' }), store: store
      )

      expect(events.map(&:class)).to eq([HandleTestMessages::DeviceActivated])
    end
  end

  describe 'optimistic concurrency' do
    it 'guard prevents concurrent writes within the same partition' do
      # Create the device first so there is history (and thus a guard query)
      Sourced.handle!(
        HandleTestDecider,
        HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      # Simulate a concurrent write to the same partition between load and append
      # by directly appending an event after the first handle!
      store.append(
        HandleTestMessages::DeviceActivated.new(payload: { device_id: 'd1' })
      )

      # A second handle! that also needs to write to the same partition
      # should detect the concurrent write. Since the decider also sees the
      # activated state, it raises an invariant error first.
      cmd = HandleTestMessages::ActivateDevice.new(payload: { device_id: 'd1' })
      expect {
        Sourced.handle!(HandleTestDecider, cmd, store: store)
      }.to raise_error(RuntimeError, 'Already active')
    end

    it 'passes guard through to store.append for conflict detection' do
      # Verify handle! plumbs the guard through by checking that the
      # guard from load is used when appending
      allow(store).to receive(:append).and_call_original

      Sourced.handle!(
        HandleTestDecider,
        HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      expect(store).to have_received(:append).with(
        anything,
        guard: having_attributes(class: Sourced::ConsistencyGuard, selector: an_instance_of(Sourced::Selector))
      )
    end
  end

  describe 'partition keys from payload and tags' do
    def happened(note, tenant_id)
      HandleMixedTestMessages::SomethingHappened.new(payload: { order_id: '01', note: }).with_tags(tenant_id:)
    end

    def add_note(tenant_id)
      HandleMixedTestMessages::AddNote.new(payload: { order_id: '01', tenant_id:, note: 'x' })
    end

    before do
      store.append([
        happened('a', 't1'), happened('b', 't2'),
        HandleMixedTestMessages::OrderTouched.new(payload: { order_id: '01' }),
        happened('c', 't1')
      ])
    end

    it 'evolves only the partition matching both keys, plus foreign partial-key events' do
      _cmd, reactor, events = Sourced.handle!(HandleMixedTestDecider, add_note('t1'), store: store)

      expect(reactor.context).to eq(notes: %w[a c], touched: true)
      expect(events.map { |e| e.payload.count }).to eq([3])
    end

    it 'loads the same partition' do
      reactor, read_result = Sourced.load(HandleMixedTestDecider, store: store, order_id: '01', tenant_id: 't2')

      expect(reactor.context).to eq(notes: %w[b], touched: true)
      expect(read_result.messages.size).to eq(2)
    end

    it 'does not conflict with a concurrent append to another tenant' do
      allow(store).to receive(:read).and_wrap_original do |original, *args, **kwargs|
        original.call(*args, **kwargs).tap { store.append(happened('concurrent', 't2')) }
      end

      expect { Sourced.handle!(HandleMixedTestDecider, add_note('t1'), store: store) }.not_to raise_error
    end

    it 'conflicts with a concurrent append to its tenant' do
      allow(store).to receive(:read).and_wrap_original do |original, *args, **kwargs|
        original.call(*args, **kwargs).tap { store.append(happened('concurrent', 't1')) }
      end

      expect {
        Sourced.handle!(HandleMixedTestDecider, add_note('t1'), store: store)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'decides with GWT helpers, given tagged history' do
      with_reactor(HandleMixedTestDecider, order_id: '01', tenant_id: 't1')
        .given(happened('a', 't1'))
        .when(add_note('t1'))
        .then(HandleMixedTestMessages::NotesCounted, order_id: '01', tenant_id: 't1', count: 2)
    end

    it 'is claimed by a worker into its partition, evolving tagged history' do
      router = Sourced::Router.new(store: store)
      router.register(HandleMixedTestDecider)
      store.append(add_note('t1'))

      nil while router.handle_next_for(HandleMixedTestDecider, worker_id: 'w-1')

      counted = store.read(Sourced::Selector[HandleMixedTestMessages::NotesCounted].by(:order_id, :tenant_id).bind(order_id: '01', tenant_id: 't1')).messages
      expect(counted.map { |e| e.payload.count }).to eq([3])
    end
  end

  it 'builds a reactor that reads no history with its partition values by key' do
    reactor = Class.new do
      class << self
        attr_accessor :built_with
      end
      def self.group_id = 'handle_test.built_with'
      def self.claim_selector = Sourced::Selector[HandleUncheckedMessages::AddNote].by(:user_id, :device_id)
      def self.handle_claim(claim) = []

      def initialize(values) = self.class.built_with = values
      def handles_command?(_cmd) = true
      def decide(_cmd) = []
    end
    command = HandleUncheckedMessages::AddNote.new(payload: { text: 'x' }).with_tags(user_id: 'u1', device_id: 'd1')

    Sourced.handle!(reactor, command, store:)
    expect(reactor.built_with).to eq(device_id: 'd1', user_id: 'u1')
  end

  it 'raises when a partition key is in neither the payload nor the tags, appending nothing' do
    expect {
      Sourced.handle!(HandleUncheckedReactor, HandleUncheckedMessages::AddNote.new(payload: { text: 'x' }), store: store)
    }.to raise_error(Sourced::MissingPartitionKeyError, /\[:device_id\]/)
    expect(store.latest_position).to eq(0)
  end

  describe 'offset advancement for registered reactors' do
    let(:router) { Sourced::Router.new(store: store) }

    before do
      router.register(HandleTestDecider)
      Sourced.register(HandleTestDecider)
    end

    after { Sourced.reset! }

    it 'advances offsets so background workers skip handled commands' do
      cmd = HandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      Sourced.handle!(HandleTestDecider, cmd, store: store)

      # Background worker should find no work for this partition
      handled = router.handle_next_for(HandleTestDecider, worker_id: 'test-worker')
      expect(handled).to be false
    end

    it 'advances offsets after multiple commands on same partition' do
      Sourced.handle!(
        HandleTestDecider,
        HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      Sourced.handle!(
        HandleTestDecider,
        HandleTestMessages::ActivateDevice.new(payload: { device_id: 'd1' }),
        store: store
      )

      # Background worker should still find no work
      handled = router.handle_next_for(HandleTestDecider, worker_id: 'test-worker')
      expect(handled).to be false
    end

    it "lets background workers run the reactor's reactions to the events it produced" do
      router.register(HandleTestReactingDecider)
      Sourced.register(HandleTestReactingDecider)

      cmd = HandleTestMessages::RegisterDevice.new(payload: { device_id: 'd1', name: 'Sensor' })
      Sourced.handle!(HandleTestReactingDecider, cmd, store: store)

      expect(router.handle_next_for(HandleTestReactingDecider, worker_id: 'test-worker')).to be true
      expect(router.handle_next_for(HandleTestReactingDecider, worker_id: 'test-worker')).to be false

      types = store.read_all(limit: 10).messages.map(&:type)
      expect(types).to eq(%w[
        handle_test.register_device
        handle_test.device_created
        handle_test.notify_created
      ])
    end

    describe 'commands handled before workers catch up' do
      before do
        router.register(HandleTestReactingDecider)
        Sourced.register(HandleTestReactingDecider)
      end

      def register(device_id, name = 'Sensor')
        HandleTestMessages::RegisterDevice.new(payload: { device_id:, name: })
      end

      def log_types = store.read_all(limit: 100).messages.map(&:type)

      it "runs reactions to every command's events in the partition" do
        Sourced.handle!(HandleTestReactingDecider, register('d1', 'a'), store:)
        Sourced.handle!(HandleTestReactingDecider, register('d1', 'b'), store:)
        router.drain

        expect(log_types.count('handle_test.notify_created')).to eq(2)
      end

      it "runs reactions to another partition's earlier events" do
        Sourced.handle!(HandleTestReactingDecider, register('d1'), store:)
        Sourced.handle!(HandleTestReactingDecider, register('d2'), store:)
        router.drain

        expect(log_types.count('handle_test.notify_created')).to eq(2)
      end

      it "doesn't decide a command again for a worker holding the partition's claim" do
        Sourced.handle!(HandleTestReactingDecider, register('d1', 'a'), store:)
        claim = store.claim_next(HandleTestReactingDecider.group_id, HandleTestReactingDecider.claim_selector, worker_id: 'w')

        Sourced.handle!(HandleTestReactingDecider, register('d1', 'b'), store:)
        store.ack(HandleTestReactingDecider.group_id, offset_id: claim.offset_id, version: claim.offset_version, position: claim.messages.last.position)
        router.drain

        expect(log_types.count('handle_test.device_created')).to eq(2)
      end

      it 'acks a claim of only marked commands, without running the reactor' do
        Sourced.handle!(HandleTestReactingDecider, register('d1', 'a'), store:)
        claim = store.claim_next(HandleTestReactingDecider.group_id, HandleTestReactingDecider.claim_selector, worker_id: 'w')
        cmd = register('d1', 'b')
        Sourced.handle!(HandleTestReactingDecider, cmd, store:)
        store.release(HandleTestReactingDecider.group_id, offset_id: claim.offset_id, version: claim.offset_version)
        store.advance_offset(HandleTestReactingDecider.group_id, partition: { 'device_id' => 'd1' }, position: claim.messages.last.position)

        count = store.latest_position
        expect(router.handle_next_for(HandleTestReactingDecider, worker_id: 'w', batch_size: 1)).to be(true)
        expect(store.latest_position).to eq(count)

        next_claim = store.claim_next(HandleTestReactingDecider.group_id, HandleTestReactingDecider.claim_selector, worker_id: 'w')
        expect(next_claim.messages.map(&:causation_id)).to eq([cmd.id])
      end

      it 'acks through a marked command that ends the batch' do
        Sourced.handle!(HandleTestReactingDecider, register('d1', 'a'), store:)
        claim = store.claim_next(HandleTestReactingDecider.group_id, HandleTestReactingDecider.claim_selector, worker_id: 'w')
        cmd = register('d1', 'b')
        Sourced.handle!(HandleTestReactingDecider, cmd, store:)
        store.release(HandleTestReactingDecider.group_id, offset_id: claim.offset_id, version: claim.offset_version)

        # [first command's event, marked command]
        expect(router.handle_next_for(HandleTestReactingDecider, worker_id: 'w', batch_size: 2)).to be(true)

        next_claim = store.claim_next(HandleTestReactingDecider.group_id, HandleTestReactingDecider.claim_selector, worker_id: 'w')
        expect(next_claim.messages.map(&:type)).to eq(%w[handle_test.device_created])
        expect(next_claim.messages.last.causation_id).to eq(cmd.id)
      end

      it 'marks the stored command, not the events correlated from it' do
        cmd = register('d1')
        result = Sourced.handle!(HandleTestReactingDecider, cmd, store:)

        stored = store.read_all(limit: 10).messages
        expect(stored.first.metadata[Sourced::HANDLED_BY_KEY]).to eq([HandleTestReactingDecider.group_id, cmd.id])
        expect(result.command.metadata[Sourced::HANDLED_BY_KEY]).to eq([HandleTestReactingDecider.group_id, cmd.id])
        expect(stored.drop(1).map { |m| m.metadata[Sourced::HANDLED_BY_KEY] }).to all(be_nil)
      end

      it "runs a machine's entry actions for every transition" do
        router.register(HandleTestDoorMachine)
        Sourced.register(HandleTestDoorMachine)

        Sourced.handle!(HandleTestDoorMachine, HandleTestDoorMachine::OpenDoor.new(payload: { door_id: 'x' }), store:)
        Sourced.handle!(HandleTestDoorMachine, HandleTestDoorMachine::CloseDoor.new(payload: { door_id: 'x' }), store:)
        router.drain

        expect(log_types.count(HandleTestDoorMachine::Alert.type)).to eq(2)
      end
    end

    it "raises when the reactor is registered but Sourced hasn't started, without appending the command" do
      unstarted_db = Sequel.sqlite
      unstarted = Sourced::Store.new(unstarted_db)
      unstarted.install!

      expect {
        Sourced.handle!(
          HandleTestDecider,
          HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
          store: unstarted
        )
      }.to raise_error(Sourced::ConsumerGroupNotRegisteredError, /HandleTestDecider.*start Sourced/)

      expect(unstarted_db[:sourced_messages].count).to eq(0)
    end

    it "doesn't advance offsets of reactors that aren't registered" do
      Sourced.reset!

      Sourced.handle!(
        HandleTestDecider,
        HandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      handled = router.handle_next_for(HandleTestDecider, worker_id: 'test-worker')
      expect(handled).to be true
    end
  end
end
