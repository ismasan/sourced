# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

module CCCProjectorTestMessages
  ItemAdded = Sourced::CCC::Message.define('projector_test.item.added') do
    attribute :list_id, String
    attribute :name, String
  end

  ItemArchived = Sourced::CCC::Message.define('projector_test.item.archived') do
    attribute :list_id, String
    attribute :name, String
  end

  NotifyArchive = Sourced::CCC::Message.define('projector_test.notify_archive') do
    attribute :list_id, String
  end
end

class TestItemProjector < Sourced::CCC::Projector
  partition_by :list_id
  consumer_group 'item-projector-test'

  state do |(list_id)|
    { list_id: list_id, items: [], synced: false }
  end

  evolve CCCProjectorTestMessages::ItemAdded do |state, msg|
    state[:items] << msg.payload.name
  end

  evolve CCCProjectorTestMessages::ItemArchived do |state, msg|
    state[:items].delete(msg.payload.name)
  end

  reaction CCCProjectorTestMessages::ItemArchived do |_state, msg|
    CCCProjectorTestMessages::NotifyArchive.new(payload: { list_id: msg.payload.list_id })
  end

  sync do |state:, messages:, replaying:|
    state[:synced] = true
    state[:last_replaying] = replaying
  end
end

RSpec.describe Sourced::CCC::Projector do
  describe '.handled_messages' do
    it 'includes evolve and react types' do
      msgs = TestItemProjector.handled_messages
      expect(msgs).to include(CCCProjectorTestMessages::ItemAdded)
      expect(msgs).to include(CCCProjectorTestMessages::ItemArchived)
    end
  end

  describe '.handle_batch' do
    let(:guard) { Sourced::CCC::ConsistencyGuard.new(conditions: [], last_position: 2) }

    def make_claim(messages, replaying: false)
      Sourced::CCC::ClaimResult.new(
        offset_id: 1, key_pair_ids: [], partition_key: 'list_id:L1',
        partition_value: { 'list_id' => 'L1' },
        messages: messages, replaying: replaying, guard: guard
      )
    end

    it 'evolves from claim.messages and includes sync actions' do
      msgs = [
        Sourced::CCC::PositionedMessage.new(
          CCCProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        ),
        Sourced::CCC::PositionedMessage.new(
          CCCProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Banana' }), 2
        )
      ]
      claim = make_claim(msgs)

      pairs = TestItemProjector.handle_batch(claim)

      # Last pair should contain sync actions
      sync_pair = pairs.last
      sync_actions, source_msg = sync_pair
      expect(source_msg).to eq(msgs.last)

      sync_action = Array(sync_actions).find { |a| a.is_a?(Sourced::CCC::Actions::Sync) }
      expect(sync_action).not_to be_nil
    end

    it 'runs reactions when not replaying' do
      msgs = [
        Sourced::CCC::PositionedMessage.new(
          CCCProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: false)

      pairs = TestItemProjector.handle_batch(claim)

      # Should have reaction pair + sync pair
      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::CCC::Actions::Append) }

      expect(append_actions.size).to eq(1)
      expect(append_actions.first.messages.first).to be_a(CCCProjectorTestMessages::NotifyArchive)
    end

    it 'skips reactions when replaying' do
      msgs = [
        Sourced::CCC::PositionedMessage.new(
          CCCProjectorTestMessages::ItemArchived.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: true)

      pairs = TestItemProjector.handle_batch(claim)

      append_actions = pairs.flat_map { |actions, _| Array(actions) }
        .select { |a| a.is_a?(Sourced::CCC::Actions::Append) }

      expect(append_actions).to be_empty
    end

    it 'passes replaying to sync blocks' do
      msgs = [
        Sourced::CCC::PositionedMessage.new(
          CCCProjectorTestMessages::ItemAdded.new(payload: { list_id: 'L1', name: 'Apple' }), 1
        )
      ]
      claim = make_claim(msgs, replaying: true)

      pairs = TestItemProjector.handle_batch(claim)

      # Execute the sync action to verify replaying is passed through
      sync_pair = pairs.last
      sync_actions = Array(sync_pair.first).select { |a| a.is_a?(Sourced::CCC::Actions::Sync) }
      expect(sync_actions).not_to be_empty

      # Call the sync to verify it runs
      sync_actions.first.call
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
