# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'

module DeciderTestMessages
  DeviceRegistered = Sourced::Message.define('decider_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  DeviceBound = Sourced::Message.define('decider_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  BindDevice = Sourced::Message.define('decider_test.bind_device') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  NotifyBound = Sourced::Message.define('decider_test.notify_bound') do
    attribute :device_id, String
  end

  DelayedNotifyBound = Sourced::Message.define('decider_test.delayed_notify_bound') do
    attribute :device_id, String
  end

  BindingRejected = Sourced::Message.define('decider_test.binding_rejected') do
    attribute :device_id, String
  end
end

class TestDeviceDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'device_decider_test'

  context { { exists: false, bound: false } }

  evolve DeciderTestMessages::DeviceRegistered do |context, _evt|
    context[:exists] = true
  end

  evolve DeciderTestMessages::DeviceBound do |context, _evt|
    context[:bound] = true
  end

  command DeciderTestMessages::BindDevice => DeciderTestMessages::DeviceBound do |context, cmd|
    raise 'Not found' unless context[:exists]
    raise 'Already bound' if context[:bound]
    event DeciderTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  reaction DeciderTestMessages::DeviceBound => DeciderTestMessages::NotifyBound do |_context, evt|
    DeciderTestMessages::NotifyBound.new(payload: { device_id: evt.payload.device_id })
  end

  after_sync do |context:, messages:, events:|
    context[:after_synced] = true
  end
end

class TestDelayedReactionDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'device_delayed_decider_test'

  context { { exists: false, bound: false } }

  evolve DeciderTestMessages::DeviceRegistered do |context, _evt|
    context[:exists] = true
  end

  evolve DeciderTestMessages::DeviceBound do |context, _evt|
    context[:bound] = true
  end

  command DeciderTestMessages::BindDevice => DeciderTestMessages::DeviceBound do |context, cmd|
    raise 'Not found' unless context[:exists]
    raise 'Already bound' if context[:bound]
    event DeciderTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  reaction DeciderTestMessages::DeviceBound => DeciderTestMessages::DelayedNotifyBound do |_context, evt|
    dispatch(DeciderTestMessages::DelayedNotifyBound, device_id: evt.payload.device_id)
      .at(Time.now + 10)
  end
end

# Reacts on replay too, by overriding the default replay gate.
class TestReplayingReactionDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'device_replaying_decider_test'

  context { { exists: false, bound: false } }

  evolve DeciderTestMessages::DeviceBound do |context, _evt|
    context[:bound] = true
  end

  # device_id echoes whether the reacted message was evolved into context first.
  reaction DeciderTestMessages::DeviceBound => DeciderTestMessages::NotifyBound do |context, evt|
    device_id = context[:bound] ? evt.payload.device_id : 'not-evolved'
    DeciderTestMessages::NotifyBound.new(payload: { device_id: device_id })
  end

  def should_react?(_context, _message, replaying: false)
    true
  end
end

# Counts occurrences so a message evolved twice is observable.
class TestCountingDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'device_counting_decider_test'

  context { { bound_count: 0 } }

  evolve DeciderTestMessages::DeviceBound do |context, _evt|
    context[:bound_count] += 1
  end

  # device_id echoes how many times the reacted message hit the evolve block.
  reaction DeciderTestMessages::DeviceBound => DeciderTestMessages::NotifyBound do |context, _evt|
    DeciderTestMessages::NotifyBound.new(payload: { device_id: "count:#{context[:bound_count]}" })
  end
end

# Refuses BindDevice until the device is registered, instead of raising.
class TestGatedDeviceDecider < TestDeviceDecider
  consumer_group 'device_gated_decider_test'

  def handles_command?(command)
    super && context[:exists]
  end
end

RSpec.describe Sourced::Decider do
  describe '.command' do
    it 'registers handler and #decide runs it' do
      expect(TestDeviceDecider.handled_commands).to include(DeciderTestMessages::BindDevice)

      instance = TestDeviceDecider.new
      instance.evolve([DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1' })])

      events = instance.decide(
        DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      expect(events.size).to eq(1)
      expect(events.first).to be_a(DeciderTestMessages::DeviceBound)
    end

    it 'takes anything whose to_message_class is a message class, and decides that class, wrapped or not' do
      declared = Object.new
      declared.define_singleton_method(:to_message_class) { DeciderTestMessages::NotifyBound }
      decider = Class.new(Sourced::Decider) do
        partition_by :device_id
        command(declared => DeciderTestMessages::DeviceRegistered) { |_context, cmd| event DeciderTestMessages::DeviceRegistered, device_id: cmd.payload.device_id }
      end
      cmd = DeciderTestMessages::NotifyBound.new(payload: { device_id: 'd1' })
      wrapped = Struct.new(:to_message) { def payload = to_message.payload }.new(cmd)

      expect(decider.handled_commands).to eq([DeciderTestMessages::NotifyBound])
      expect(decider.new.handles_command?(wrapped)).to be(true)
      expect(decider.new.decide(wrapped).map(&:class)).to eq([DeciderTestMessages::DeviceRegistered])
    end

    it 'takes only what answers to_message_class' do
      expect { Class.new(Sourced::Decider) { command(:bind => nil) {} } }
        .to raise_error(NoMethodError, /to_message_class/)
    end
  end

  describe 'declared outputs' do
    let(:bound) { DeciderTestMessages::DeviceBound }
    let(:notify) { DeciderTestMessages::NotifyBound }
    let(:bind) { DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' }) }

    def decider_with(outputs, &handler)
      Class.new(Sourced::Decider) do
        partition_by :device_id
        command(DeciderTestMessages::BindDevice => outputs, &handler)
      end
    end

    def decide(decider) = decider.new(device_id: 'd1').decide(bind)

    it 'takes one pair' do
      cmd = DeciderTestMessages::BindDevice
      expect { Class.new(Sourced::Decider) { command(cmd) { nil } } }
        .to raise_error(ArgumentError, /\.command takes one pair, ex. Command => Event \| Other \| nil; got DeciderTestMessages::BindDevice/)
      expect { Class.new(Sourced::Decider) { command(cmd => nil, Object => nil) { nil } } }
        .to raise_error(ArgumentError, /\.command takes one pair/)
      expect { decider_with('nope') { nil } }.to raise_error(ArgumentError, /BindDevice: "nope" isn't a message class/)
    end

    it 'flattens unions, with nil for "may emit none"' do
      decider = decider_with(bound | notify | nil) { |*| nil }

      expect(decider.command_outputs[DeciderTestMessages::BindDevice])
        .to have_attributes(messages: [bound, notify], optional: true)
      expect(decider.produced_messages).to eq([bound, notify])
      expect(decide(decider)).to eq([])
    end

    it 'emits several declared messages' do
      decider = decider_with(bound | notify) do |_, cmd|
        event DeciderTestMessages::NotifyBound, device_id: cmd.payload.device_id
        event DeciderTestMessages::DeviceBound, **cmd.payload.to_h
      end

      expect(decide(decider).map(&:class)).to eq([notify, bound])
    end

    it "raises when the handler emits a message its command doesn't declare" do
      decider = decider_with(bound) do |_, cmd|
        event DeciderTestMessages::NotifyBound, device_id: cmd.payload.device_id
      end

      expect { decide(decider) }
        .to raise_error(Sourced::Decider::UndeclaredEventError, /emitted .*NotifyBound, which its command doesn't/)
    end

    it 'raises when the handler emits nothing, unless its outputs include nil' do
      expect { decide(decider_with(bound) { |*| nil }) }
        .to raise_error(Sourced::Decider::UndeclaredEventError, /emitted nothing; declare .* \| nil/)
    end

    it 'is inherited' do
      child = Class.new(decider_with(bound) { |*| nil })

      expect(child.command_outputs[DeciderTestMessages::BindDevice].messages).to eq([bound])
    end
  end

  describe '#event inside command handler' do
    it 'adds to uncommitted events and evolves context immediately' do
      instance = TestDeviceDecider.new
      instance.evolve([DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1' })])

      events = instance.decide(
        DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      expect(events.size).to eq(1)
      expect(instance.context[:bound]).to be true
    end
  end

  describe '.handle_claim' do
    let(:db) { Sequel.sqlite }
    let(:store) { Sourced::Store.new(db) }

    before do
      store.install!
    end

    it 'evolves from history, decides commands, returns action pairs' do
      # Set up history
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 1)
      history = Sourced::ReadResult.new(messages: history_msgs, guard: guard)

      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      cmd_positioned = Sourced::PositionedMessage.new(cmd, 2)

      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [cmd_positioned], replaying: false, guard: guard
      )

      pairs = TestDeviceDecider.handle_claim(claim, history: history)

      # Should have action pairs from the command
      expect(pairs).to be_a(Array)
      expect(pairs.size).to eq(1) # one command processed

      actions, source_msg = pairs.first
      expect(source_msg).to eq(cmd_positioned)

      # Reactions are deferred: command claim produces only the event Append (+ after_sync).
      # The reaction runs when DeviceBound is re-claimed by this Decider.
      append_actions = Array(actions).select { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append_actions.size).to eq(1)

      event_append = append_actions.first
      expect(event_append.messages.first).to be_a(DeciderTestMessages::DeviceBound)
      expect(event_append.guard).to eq(guard)

      # No reaction Append in this pass.
      reaction_types = append_actions.flat_map { |a| a.messages.map(&:class) }
      expect(reaction_types).not_to include(DeciderTestMessages::NotifyBound)
    end

    it 'runs deferred reactions when the triggering event is re-claimed' do
      # History: device was already registered and bound (context reflects both)
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      bound = DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })
      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 2)
      history = Sourced::ReadResult.new(messages: history_msgs, guard: guard)

      bound_positioned = Sourced::PositionedMessage.new(bound, 2)
      claim = Sourced::ClaimResult.new(
        offset_id: 2, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [bound_positioned], replaying: false, guard: guard
      )

      pairs = TestDeviceDecider.handle_claim(claim, history: history)
      expect(pairs.size).to eq(1)

      actions, source_msg = pairs.first
      expect(source_msg).to eq(bound_positioned)

      append_actions = Array(actions).select { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append_actions.size).to eq(1)

      reaction_append = append_actions.first
      expect(reaction_append.messages.first).to be_a(DeciderTestMessages::NotifyBound)
      expect(reaction_append.source).to eq(bound_positioned)
      expect(reaction_append.guard).to be_nil

      after_sync = Array(actions).find { |a| a.is_a?(Sourced::Actions::AfterSync) }
      expect(after_sync).not_to be_nil
    end

    it 'skips reactions on replaying claims' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      bound = DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })
      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 2)
      history = Sourced::ReadResult.new(messages: history_msgs, guard: guard)

      bound_positioned = Sourced::PositionedMessage.new(bound, 2)
      claim = Sourced::ClaimResult.new(
        offset_id: 2, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [bound_positioned], replaying: true, guard: guard
      )

      pairs = TestDeviceDecider.handle_claim(claim, history: history)
      expect(pairs.size).to eq(1)

      actions, source_msg = pairs.first
      expect(actions).to eq(Sourced::Actions::OK)
      expect(source_msg).to eq(bound_positioned)
    end

    it 'includes after_sync actions in action pairs' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 1)
      history = Sourced::ReadResult.new(messages: history_msgs, guard: guard)

      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      cmd_positioned = Sourced::PositionedMessage.new(cmd, 2)

      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [cmd_positioned], replaying: false, guard: guard
      )

      pairs = TestDeviceDecider.handle_claim(claim, history: history)
      actions = Array(pairs.first.first)

      after_sync_action = actions.find { |a| a.is_a?(Sourced::Actions::AfterSync) }
      expect(after_sync_action).not_to be_nil
    end

    it 'binds events: in sync hooks to the events the runner appended, correlated' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 0)
      history = Sourced::ReadResult.new(messages: [], guard: guard)

      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      cmd_positioned = Sourced::PositionedMessage.new(cmd, 2)

      seen_sync = []
      seen_after = []
      decider = Class.new(TestDeviceDecider) do
        consumer_group 'device_decider_correlation_test'
        sync { |events:, **| seen_sync.concat(events) }
        after_sync { |events:, **| seen_after.concat(events) }
      end

      # Runner over a real store, as the router drives it.
      store = Sourced::Store.new(Sequel.sqlite)
      store.install!
      store.append(reg)
      history = store.read(Sourced::Selector[DeciderTestMessages::DeviceRegistered].by(:device_id).bind(device_id: 'd1'))
      pairs = decider.handle_batch({ device_id: 'd1' }, [cmd_positioned], history:)

      after_syncs = []
      runner = Sourced::ActionRunner.new(store)
      pairs.each { |signals, source| runner.run_pair(signals, source, after_syncs) }
      after_syncs.each(&:call)

      stored = store.read(Sourced::Selector[DeciderTestMessages::DeviceBound].by(:device_id).bind(device_id: 'd1')).messages
      expect(stored.size).to eq(1)
      expect(seen_sync).to eq(stored)
      expect(seen_after).to eq(stored)
      expect(seen_sync.first.causation_id).to eq(cmd.id)
      expect(seen_sync.first.correlation_type).to eq(cmd.type)
      expect(seen_sync.first).to equal(seen_after.first)
    end

    it 'returns [OK, msg] for non-command messages' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      reg_positioned = Sourced::PositionedMessage.new(reg, 1)

      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 0)
      history = Sourced::ReadResult.new(messages: [], guard: guard)

      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [reg_positioned], replaying: false, guard: guard
      )

      pairs = TestDeviceDecider.handle_claim(claim, history: history)

      expect(pairs.size).to eq(1)
      actions, source_msg = pairs.first
      expect(actions).to eq(Sourced::Actions::OK)
      expect(source_msg).to eq(reg_positioned)
    end

    it 'wraps delayed reaction dispatches in an append action carrying a future-dated message' do
      # Reactions are deferred, so the delayed-dispatch only appears when
      # DeviceBound is re-claimed by the Decider, not on the command claim.
      # The store defers the future-dated message to scheduled_messages on append.
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      bound = DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })
      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 2)
      history = Sourced::ReadResult.new(messages: history_msgs, guard: guard)

      bound_positioned = Sourced::PositionedMessage.new(bound, 2)
      claim = Sourced::ClaimResult.new(
        offset_id: 2, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [bound_positioned],
        replaying: false,
        guard: guard
      )

      pairs = TestDelayedReactionDecider.handle_claim(claim, history: history)
      actions = pairs.first.first
      append_action = Array(actions).find { |action| action.is_a?(Sourced::Actions::Append) }

      expect(append_action).not_to be_nil
      delayed = append_action.messages.find { |m| m.is_a?(DeciderTestMessages::DelayedNotifyBound) }
      expect(delayed).not_to be_nil
      expect(delayed.created_at).to be > Time.now
    end

    it 'invariant violation propagates as error' do
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 0)
      history = Sourced::ReadResult.new(messages: [], guard: guard)

      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      cmd_positioned = Sourced::PositionedMessage.new(cmd, 1)

      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [cmd_positioned], replaying: false, guard: guard
      )

      # No history → context[:exists] is false → raises 'Not found'
      expect {
        TestDeviceDecider.handle_claim(claim, history: history)
      }.to raise_error(RuntimeError, 'Not found')
    end
  end

  describe '#should_react?' do
    let(:guard) { Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 2) }

    def bound_claim(replaying:)
      bound = Sourced::PositionedMessage.new(
        DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' }), 2
      )
      claim = Sourced::ClaimResult.new(
        offset_id: 2, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [bound], replaying: replaying, guard: guard
      )
      [claim, bound]
    end

    it 'defaults to skipping reactions while replaying' do
      instance = TestDeviceDecider.new(device_id: 'd1')
      msg = DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' })

      expect(instance.should_react?(instance.context, msg, replaying: false)).to be(true)
      expect(instance.should_react?(instance.context, msg, replaying: true)).to be(false)
    end

    it 'runs reactions on replaying claims when overridden to true' do
      claim, bound = bound_claim(replaying: true)
      history = Sourced::ReadResult.new(messages: [], guard: guard)

      pairs = TestReplayingReactionDecider.handle_claim(claim, history: history)

      actions, source_msg = pairs.first
      expect(source_msg).to eq(bound)

      append = Array(actions).find { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append.messages.first).to be_a(DeciderTestMessages::NotifyBound)
      expect(append.source).to eq(bound)
    end

    it 'evolves context with the reacted message when reacting on replay' do
      claim, _bound = bound_claim(replaying: true)
      history = Sourced::ReadResult.new(messages: [], guard: guard)

      pairs = TestReplayingReactionDecider.handle_claim(claim, history: history)

      actions, _source_msg = pairs.first
      append = Array(actions).find { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append.messages.first.payload.device_id).to eq('d1')
    end
  end

  describe '#handles_command?' do
    include Sourced::Testing::RSpec

    it 'defaults to whether a handler is registered for the command class' do
      instance = TestDeviceDecider.new(device_id: 'd1')

      expect(instance.handles_command?(DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' }))).to be(true)
      expect(instance.handles_command?(DeciderTestMessages::NotifyBound.new(payload: { device_id: 'd1' }))).to be(false)
    end

    it 'acks a command it does not handle in its current context, with no events' do
      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })

      with_reactor(TestGatedDeviceDecider, device_id: 'd1')
        .when(cmd)
        .then do |result|
          expect(result.pairs).to eq([[Sourced::Actions::OK, cmd]])
          expect(result.messages).to be_empty
        end
    end

    it 'decides the command once its context allows it' do
      with_reactor(TestGatedDeviceDecider, device_id: 'd1')
        .given(DeciderTestMessages::DeviceRegistered, device_id: 'd1', name: 'Sensor')
        .when(DeciderTestMessages::BindDevice, device_id: 'd1', asset_id: 'a1')
        .then(DeciderTestMessages::DeviceBound, device_id: 'd1', asset_id: 'a1')
    end
  end

  describe '.handle_claim history deduplication' do
    let(:guard) { Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 2) }

    it 'evolves a claimed message once, even though the unbounded read includes it' do
      bound = Sourced::PositionedMessage.new(
        DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' }), 2
      )
      # Store#read is unbounded: history contains the claimed message.
      history = Sourced::ReadResult.new(messages: [bound], guard: guard)
      claim = Sourced::ClaimResult.new(
        offset_id: 2, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [bound], replaying: false, guard: guard
      )

      pairs = TestCountingDecider.handle_claim(claim, history: history)

      actions, _source_msg = pairs.first
      append = Array(actions).find { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append.messages.first.payload.device_id).to eq('count:1')
    end

    it 'keeps the guard from the full read' do
      bound = Sourced::PositionedMessage.new(
        DeciderTestMessages::DeviceBound.new(payload: { device_id: 'd1', asset_id: 'a1' }), 2
      )
      history = Sourced::ReadResult.new(messages: [bound], guard: guard)

      expect(history.excluding([bound]).guard).to eq(guard)
      expect(history.excluding([bound]).messages).to be_empty
    end
  end

  describe '.handled_messages' do
    it 'includes commands and react types but not evolve types' do
      msgs = TestDeviceDecider.handled_messages
      expect(msgs).to include(DeciderTestMessages::BindDevice)
      expect(msgs).to include(DeciderTestMessages::DeviceBound) # reaction
      expect(msgs).not_to include(DeciderTestMessages::DeviceRegistered) # evolve only
    end
  end

  describe '.history_selector' do
    it "is its projection's selector, by the partition keys" do
      selector = TestDeviceDecider.history_selector

      expect(selector).to eq(TestDeviceDecider.projector.selector.by(:device_id))
      expect(selector.types.map(&:type)).to include('decider_test.device.registered', 'decider_test.device.bound')
    end
  end

  describe 'inheritance' do
    it 'subclass inherits command handlers' do
      subclass = Class.new(TestDeviceDecider)
      expect(subclass.handled_commands).to include(DeciderTestMessages::BindDevice)
    end

    it 'subclass inherits partition keys unless it declares its own' do
      expect(Class.new(TestDeviceDecider).partition_keys).to eq([:device_id])
      expect(Class.new(TestDeviceDecider) { partition_by :other_id }.partition_keys).to eq([:other_id])
      expect(TestDeviceDecider.partition_keys).to eq([:device_id])
    end
  end

  describe 'assertions over a selector source, in a handler' do
    let(:registered) { DeciderTestMessages::DeviceRegistered }
    let(:bound) { DeciderTestMessages::DeviceBound }
    let(:rejected) { DeciderTestMessages::BindingRejected }

    let(:binder) do
      bindable = Sourced::Assertion.includes(registered) + Sourced::Assertion.excludes(bound)
      Class.new(Sourced::Decider) do
        partition_by :device_id
        consumer_group 'decider_test.binder'
        evolve Sourced::Selector[DeciderTestMessages::DeviceRegistered, DeciderTestMessages::DeviceBound]

        command DeciderTestMessages::BindDevice => DeciderTestMessages::DeviceBound | DeciderTestMessages::BindingRejected do |history, cmd|
          next event(DeciderTestMessages::BindingRejected, device_id: cmd.payload.device_id) unless bindable === history

          event DeciderTestMessages::DeviceBound, **cmd.payload.to_h
        end
      end
    end

    def reg(device_id = 'd1') = registered.new(payload: { device_id:, name: 'Sensor' })
    def bind(device_id = 'd1') = DeciderTestMessages::BindDevice.new(payload: { device_id:, asset_id: 'a1' })

    it "let the handler emit a domain error over the partition's history" do
      expect(binder.call([], bind).map(&:class)).to eq([rejected])
      expect(binder.call([reg], bind).map(&:class)).to eq([bound])
      expect(binder.call([reg, bound.new(payload: { device_id: 'd1', asset_id: 'a0' })], bind).map(&:class)).to eq([rejected])
    end

    it 'see what the handler emitted earlier in a batch' do
      guard = Sourced::ConsistencyGuard.new(selector: Sourced::Selector::NONE, last_position: 1)
      history = Sourced::ReadResult.new(messages: [Sourced::PositionedMessage.new(reg, 1)], guard:)
      batch = [Sourced::PositionedMessage.new(bind, 2), Sourced::PositionedMessage.new(bind, 3)]

      pairs = binder.handle_batch({ device_id: 'd1' }, batch, history:)
      appended = pairs.map { |actions, _| Array(actions).grep(Sourced::Actions::Append).flat_map(&:messages) }

      expect(appended.map { |msgs| msgs.map(&:class) }).to eq([[bound], [rejected]])
    end
  end

  describe '.call' do
    it 'decides a command from history, without a store' do
      history = [DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })]
      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })

      expect(TestDeviceDecider.call(history, cmd).map(&:class)).to eq([DeciderTestMessages::DeviceBound])
      read = Sourced::ReadResult.new(messages: history, guard: nil)
      expect(TestDeviceDecider.call(read, cmd).size).to eq(1)
    end

    it 'raises for a command without a value for a partition key' do
      cmd = DeciderTestMessages::BindDevice.new(payload: { asset_id: 'a1' })

      expect { TestDeviceDecider.call([], cmd) }.to raise_error(Sourced::MissingPartitionKeyError, /device_id/)
    end
  end

  describe 'Sourced::Decider(source, Command => outputs)' do
    let(:registered) { DeciderTestMessages::DeviceRegistered }
    let(:bound) { DeciderTestMessages::DeviceBound }
    let(:bind_cmd) { DeciderTestMessages::BindDevice }
    let(:bind) { bind_cmd.new(payload: { device_id: 'd1', asset_id: 'a1' }) }
    let(:reg) { registered.new(payload: { device_id: 'd1', name: 'Sensor' }) }

    it "is a one-command Decider over the source, grouped by the command's type, keyed by the source's keys" do
      source = Sourced::Selector[registered].by(:device_id)
      decision = Sourced::Decider(source, bind_cmd => bound) do |history, cmd|
        event DeciderTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: "#{history.size}"
      end

      expect(decision).to be < Sourced::Decider
      expect(decision).to have_attributes(
        group_id: bind_cmd.type, partition_keys: [:device_id], handled_commands: [bind_cmd],
        source:, history_selector: source, exclusive?: false
      )
      expect(decision.call([reg], bind).map { it.payload.asset_id }).to eq(['1'])
    end

    it 'chains partition_by, for a source without keys' do
      decision = Sourced::Decider(TestDeviceDecider.projector, bind_cmd => bound | nil) { |*| nil }
                   .partition_by(:device_id)

      expect(decision.partition_keys).to eq([:device_id])
      expect(decision.history_selector).to eq(TestDeviceDecider.projector.selector.by(:device_id))
      expect(decision).not_to be_exclusive
    end

    it 'takes no source: the block gets the command only, and without keys it is an id-partitioned queue' do
      decision = Sourced::Decider(bind_cmd => bound) do |cmd|
        event DeciderTestMessages::DeviceBound, **cmd.payload.to_h
      end

      expect(decision.call([], bind).map(&:class)).to eq([bound])
      expect(decision).to be_exclusive
      expect(decision.history_selector).to eq(Sourced::Selector::NONE)
      expect(decision.claim_selector).to eq(Sourced::Selector[bind_cmd].by_message_id)
      expect(decision.partition_by(:device_id)).not_to be_exclusive
    end

    it 'registers with a Router' do
      decision = Sourced::Decider(Sourced::Selector[registered].by(:device_id), bind_cmd => bound) { |*| nil }

      store = Sourced::Store.new(Sequel.sqlite)
      expect { Sourced::Router.new(store:, reactors: [decision]) }.not_to raise_error
    end

    it 'takes one pair, as command does' do
      expect { Sourced::Decider(Sourced::Selector[registered]) { nil } }
        .to raise_error(ArgumentError, /Sourced::Decider takes one pair, ex. Command => Event \| Other \| nil; got #<Sourced::Selector/)
    end
  end

  describe 'one-argument command handlers' do
    it 'can return early' do
      decider = Class.new(Sourced::Decider) do
        partition_by :device_id
        command DeciderTestMessages::BindDevice => DeciderTestMessages::DeviceBound | nil do |cmd|
          return if cmd.payload.asset_id == 'skip'

          event DeciderTestMessages::DeviceBound, **cmd.payload.to_h
        end
      end
      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'skip' })

      expect(decider.new(device_id: 'd1').decide(cmd)).to eq([])
    end
  end
end
