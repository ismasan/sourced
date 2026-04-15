# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

module ProjectorTestMessages
  ItemAdded = Sourced::Message.define('projector_test.item.added') do
    attribute :list_id, String
    attribute :name, String
  end

  ItemArchived = Sourced::Message.define('projector_test.item.archived') do
    attribute :list_id, String
    attribute :name, String
  end

  NotifyArchive = Sourced::Message.define('projector_test.notify_archive') do
    attribute :list_id, String
  end

  DelayedNotifyArchive = Sourced::Message.define('projector_test.delayed_notify_archive') do
    attribute :list_id, String
  end
end

class TestItemProjector < Sourced::Projector::StateStored
  partition_by :list_id
  consumer_group 'item-projector-test'

  state do |(list_id)|
    { list_id: list_id, items: [], synced: false }
  end

  evolve ProjectorTestMessages::ItemAdded do |state, msg|
    state[:items] << msg.payload.name
  end

  evolve ProjectorTestMessages::ItemArchived do |state, msg|
    state[:items].delete(msg.payload.name)
  end

  reaction ProjectorTestMessages::ItemArchived do |_state, msg|
    ProjectorTestMessages::NotifyArchive.new(payload: { list_id: msg.payload.list_id })
  end

  sync do |state:, messages:, replaying:|
    state[:synced] = true
    state[:last_replaying] = replaying
  end

  after_sync do |state:, messages:, replaying:|
    state[:after_synced] = true
  end
end

class TestItemESProjector < Sourced::Projector::EventSourced
  partition_by :list_id
  consumer_group 'item-es-projector-test'

  state do |(list_id)|
    { list_id: list_id, items: [], synced: false }
  end

  evolve ProjectorTestMessages::ItemAdded do |state, msg|
    state[:items] << msg.payload.name
  end

  evolve ProjectorTestMessages::ItemArchived do |state, msg|
    state[:items].delete(msg.payload.name)
  end

  reaction ProjectorTestMessages::ItemArchived do |_state, msg|
    ProjectorTestMessages::NotifyArchive.new(payload: { list_id: msg.payload.list_id })
  end

  sync do |state:, messages:, replaying:|
    state[:synced] = true
    state[:last_replaying] = replaying
  end

  after_sync do |state:, messages:, replaying:|
    state[:after_synced] = true
  end
end

class TestDelayedItemProjector < Sourced::Projector::StateStored
  partition_by :list_id
  consumer_group 'delayed-item-projector-test'

  state do |(list_id)|
    { list_id: list_id, items: [] }
  end

  evolve ProjectorTestMessages::ItemArchived do |state, msg|
    state[:items].delete(msg.payload.name)
  end

  reaction ProjectorTestMessages::ItemArchived do |_state, msg|
    dispatch(ProjectorTestMessages::DelayedNotifyArchive, list_id: msg.payload.list_id)
      .at(Time.now + 10)
  end
end

RSpec.describe Sourced::Projector do
  describe '.handled_messages' do
    it 'includes evolve and react types' do
      msgs = TestItemProjector.handled_messages
      expect(msgs).to include(ProjectorTestMessages::ItemAdded)
      expect(msgs).to include(ProjectorTestMessages::ItemArchived)
    end
  end

  describe '.handle_batch (StateStored)' do
    it 'evolves from new_messages and includes sync and after_sync actions' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Banana' }), 2
        )
      ]

      pairs = TestItemProjector.handle_batch(['L1'], msgs)

      sync_pair = pairs.last
      sync_actions, source_msg = sync_pair
      expect(source_msg).to eq(msgs.last)

      sync_action = Array(sync_actions).find { |a| a.is_a?(Sourced::Actions::Sync) }
      expect(sync_action).not_to be_nil

      after_sync_action = Array(sync_actions).find { |a| a.is_a?(Sourced::Actions::AfterSync) }
      expect(after_sync_action).not_to be_nil
    end

    it 'runs reactions when not replaying' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]

      pairs = TestItemProjector.handle_batch(['L1'], msgs)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions.size).to eq(1)
      expect(append_actions.first.messages.first).to be_a(ProjectorTestMessages::NotifyArchive)
    end

    it 'skips reactions when replaying' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]

      pairs = TestItemProjector.handle_batch(['L1'], msgs, replaying: true)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions).to be_empty
    end
  end

  describe '.handle_batch (EventSourced)' do
    let(:guard) { Sourced::ConsistencyGuard.new(conditions: [], last_position: 5) }

    def make_history(messages)
      Sourced::ReadResult.new(messages: messages, guard: guard)
    end

    it 'evolves from full history, not just new messages' do
      history_msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Banana' }), 2
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 3
        )
      ]
      new_msgs = [history_msgs.last]
      history = make_history(history_msgs)

      pairs = TestItemESProjector.handle_batch(['L1'], new_msgs, history: history)

      sync_pair = pairs.last
      _sync_actions, source_msg = sync_pair
      expect(source_msg).to eq(new_msgs.last)
    end

    it 'runs reactions only on new messages, not full history' do
      history_msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Old' }), 1
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'New' }), 2
        )
      ]
      new_msgs = [history_msgs.last]
      history = make_history(history_msgs)

      pairs = TestItemESProjector.handle_batch(['L1'], new_msgs, history: history)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions.size).to eq(1)
    end

    it 'skips reactions when replaying' do
      history_msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      history = make_history(history_msgs)

      pairs = TestItemESProjector.handle_batch(['L1'], history_msgs, history: history, replaying: true)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions).to be_empty
    end
  end

  describe '.handle_claim' do
    let(:guard) { Sourced::ConsistencyGuard.new(conditions: [], last_position: 2) }

    def make_claim(messages, replaying: false)
      Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'list_id:L1',
        partition_value: { 'list_id' => 'L1' },
        messages: messages, replaying: replaying, guard: guard
      )
    end

    it 'evolves from claim.messages and includes sync actions' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Banana' }), 2
        )
      ]
      claim = make_claim(msgs)

      pairs = TestItemProjector.handle_claim(claim)

      # Last pair should contain sync actions
      sync_pair = pairs.last
      sync_actions, source_msg = sync_pair
      expect(source_msg).to eq(msgs.last)

      sync_action = Array(sync_actions).find { |a| a.is_a?(Sourced::Actions::Sync) }
      expect(sync_action).not_to be_nil
    end

    it 'runs reactions when not replaying' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: false)

      pairs = TestItemProjector.handle_claim(claim)

      # Should have reaction pair + sync pair
      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions.size).to eq(1)
      expect(append_actions.first.messages.first).to be_a(ProjectorTestMessages::NotifyArchive)
    end

    it 'returns schedule actions for delayed reactions' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: false)

      pairs = TestDelayedItemProjector.handle_claim(claim)

      schedule_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |action| action.is_a?(Sourced::Actions::Schedule) }

      expect(schedule_actions.size).to eq(1)
      expect(schedule_actions.first.messages.first).to be_a(ProjectorTestMessages::DelayedNotifyArchive)
    end

    it 'skips reactions when replaying' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: true)

      pairs = TestItemProjector.handle_claim(claim)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions).to be_empty
    end

    it 'passes replaying to sync blocks' do
      msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: true)

      pairs = TestItemProjector.handle_claim(claim)

      # Execute the sync action to verify replaying is passed through
      sync_pair = pairs.last
      sync_actions = Array(sync_pair.first).select { |a| a.is_a?(Sourced::Actions::Sync) }
      expect(sync_actions).not_to be_empty

      # Call the sync to verify it runs
      sync_actions.first.call
    end
  end

  describe 'EventSourced' do
    let(:guard) { Sourced::ConsistencyGuard.new(conditions: [], last_position: 5) }

    def make_claim(messages, replaying: false)
      Sourced::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'list_id:L1',
        partition_value: { 'list_id' => 'L1' },
        messages: messages, replaying: replaying, guard: guard
      )
    end

    def make_history(messages)
      Sourced::ReadResult.new(messages: messages, guard: guard)
    end

    it 'evolves from full history, not just claim messages' do
      history_msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Banana' }), 2
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 3
        )
      ]
      # Claim only contains the latest message
      claim_msgs = [history_msgs.last]
      claim = make_claim(claim_msgs)
      history = make_history(history_msgs)

      pairs = TestItemESProjector.handle_claim(claim, history: history)

      # Sync pair should be the last one, acked against claim's last message
      sync_pair = pairs.last
      _sync_actions, source_msg = sync_pair
      expect(source_msg).to eq(claim_msgs.last)
    end

    it 'runs reactions only on claim messages, not full history' do
      history_msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Old' }), 1
        ),
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'New' }), 2
        )
      ]
      # Only the second message is in the claim
      claim_msgs = [history_msgs.last]
      claim = make_claim(claim_msgs, replaying: false)
      history = make_history(history_msgs)

      pairs = TestItemESProjector.handle_claim(claim, history: history)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      # Only 1 reaction (for the claim message), not 2
      expect(append_actions.size).to eq(1)
      expect(append_actions.first.messages.first).to be_a(ProjectorTestMessages::NotifyArchive)
    end

    it 'skips reactions when replaying' do
      history_msgs = [
        Sourced::PositionedMessage.new(
          ProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(history_msgs, replaying: true)
      history = make_history(history_msgs)

      pairs = TestItemESProjector.handle_claim(claim, history: history)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::Actions::Append) }

      expect(append_actions).to be_empty
    end

    it 'is detected by Injector as needing history' do
      needs = Sourced::Injector.resolve_args(TestItemESProjector, :handle_claim)
      expect(needs).to include(:history)
    end

    it 'StateStored is not detected as needing history' do
      needs = Sourced::Injector.resolve_args(TestItemProjector, :handle_claim)
      expect(needs).not_to include(:history)
    end
  end

  describe '.context_for' do
    it 'builds conditions from partition_keys × handled_messages_for_evolve' do
      conditions = TestItemProjector.context_for(list_id: 'L1')
      types = conditions.map(&:message_type).sort
      expect(types).to include('projector_test.item.added')
      expect(types).to include('projector_test.item.archived')
    end
  end
end
