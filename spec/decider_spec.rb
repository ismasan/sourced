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

  SymbolicBound = Sourced::Message.define('decider_test.symbolic_bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  BindDeviceWithSymbol = Sourced::Message.define('decider_test.bind_device_with_symbol') do
    attribute :device_id, String
    attribute :asset_id, String
  end
end

class TestDeviceDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'device-decider-test'

  state { |_| { exists: false, bound: false } }

  evolve DeciderTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  evolve DeciderTestMessages::DeviceBound do |state, _evt|
    state[:bound] = true
  end

  command DeciderTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event DeciderTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  command DeciderTestMessages::BindDeviceWithSymbol do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event :decider_test_symbolic_bound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  reaction DeciderTestMessages::DeviceBound do |_state, evt|
    DeciderTestMessages::NotifyBound.new(payload: { device_id: evt.payload.device_id })
  end

  after_sync do |state:, messages:, events:|
    state[:after_synced] = true
  end
end

class TestDelayedReactionDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'device-delayed-decider-test'

  state { |_| { exists: false, bound: false } }

  evolve DeciderTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  evolve DeciderTestMessages::DeviceBound do |state, _evt|
    state[:bound] = true
  end

  command DeciderTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event DeciderTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  reaction DeciderTestMessages::DeviceBound do |_state, evt|
    dispatch(DeciderTestMessages::DelayedNotifyBound, device_id: evt.payload.device_id)
      .at(Time.now + 10)
  end
end

RSpec.describe Sourced::Decider do
  describe '.command' do
    it 'registers handler and #decide runs it' do
      expect(TestDeviceDecider.handled_commands).to include(DeciderTestMessages::BindDevice)
      expect(TestDeviceDecider.handled_commands).to include(DeciderTestMessages::BindDeviceWithSymbol)

      instance = TestDeviceDecider.new
      instance.instance_variable_set(:@state, { exists: true, bound: false })

      events = instance.decide(
        DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      expect(events.size).to eq(1)
      expect(events.first).to be_a(DeciderTestMessages::DeviceBound)
    end
  end

  describe '#event inside command handler' do
    it 'adds to uncommitted events and evolves state immediately' do
      instance = TestDeviceDecider.new
      instance.instance_variable_set(:@state, { exists: true, bound: false })

      events = instance.decide(
        DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      expect(events.size).to eq(1)
      expect(instance.state[:bound]).to be true
    end

    it 'resolves event classes from symbols' do
      instance = TestDeviceDecider.new
      instance.instance_variable_set(:@state, { exists: true, bound: false })

      events = instance.decide(
        DeciderTestMessages::BindDeviceWithSymbol.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      expect(events.size).to eq(1)
      expect(events.first).to be_a(DeciderTestMessages::SymbolicBound)
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
      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 1)
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

      # Actions: Append(events with guard), Append(reactions), possibly sync
      append_actions = Array(actions).select { |a| a.is_a?(Sourced::Actions::Append) }
      expect(append_actions.size).to be >= 1

      # First append has the events with guard
      event_append = append_actions.first
      expect(event_append.messages.first).to be_a(DeciderTestMessages::DeviceBound)
      expect(event_append.guard).to eq(guard)

      # Second append has the reactions (no guard)
      if append_actions.size > 1
        reaction_append = append_actions[1]
        expect(reaction_append.messages.first).to be_a(DeciderTestMessages::NotifyBound)
        expect(reaction_append.guard).to be_nil
      end
    end

    it 'includes after_sync actions in action pairs' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 1)
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

    it 'returns [OK, msg] for non-command messages' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      reg_positioned = Sourced::PositionedMessage.new(reg, 1)

      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 0)
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

    it 'returns schedule actions for delayed reaction dispatches' do
      reg = DeciderTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      history_msgs = [Sourced::PositionedMessage.new(reg, 1)]
      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 1)
      history = Sourced::ReadResult.new(messages: history_msgs, guard: guard)

      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [Sourced::PositionedMessage.new(cmd, 2)],
        replaying: false,
        guard: guard
      )

      pairs = TestDelayedReactionDecider.handle_claim(claim, history: history)
      actions = pairs.first.first
      schedule_action = Array(actions).find { |action| action.is_a?(Sourced::Actions::Schedule) }

      expect(schedule_action).not_to be_nil
      expect(schedule_action.messages.first).to be_a(DeciderTestMessages::DelayedNotifyBound)
    end

    it 'invariant violation propagates as error' do
      guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 0)
      history = Sourced::ReadResult.new(messages: [], guard: guard)

      cmd = DeciderTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      cmd_positioned = Sourced::PositionedMessage.new(cmd, 1)

      claim = Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'device_id:d1',
        partition_value: { 'device_id' => 'd1' },
        messages: [cmd_positioned], replaying: false, guard: guard
      )

      # No history → state[:exists] is false → raises 'Not found'
      expect {
        TestDeviceDecider.handle_claim(claim, history: history)
      }.to raise_error(RuntimeError, 'Not found')
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

  describe '.context_for' do
    it 'builds conditions from partition_keys × handled_messages_for_evolve' do
      conditions = TestDeviceDecider.context_for(device_id: 'd1')

      # DeviceRegistered and DeviceBound both have device_id
      types = conditions.map(&:message_type).uniq.sort
      expect(types).to include('decider_test.device.registered')
      expect(types).to include('decider_test.device.bound')
      expect(conditions.all? { |c| c.attrs[:device_id] == 'd1' }).to be true
    end
  end

  describe 'inheritance' do
    it 'subclass inherits command handlers' do
      subclass = Class.new(TestDeviceDecider)
      expect(subclass.handled_commands).to include(DeciderTestMessages::BindDevice)
    end
  end
end
