# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

# Define test messages for store specs (namespaced to avoid collisions)
module CCCStoreTestMessages
  DeviceRegistered = Sourced::CCC::Message.define('store_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  AssetRegistered = Sourced::CCC::Message.define('store_test.asset.registered') do
    attribute :asset_id, String
    attribute :label, String
  end

  DeviceBound = Sourced::CCC::Message.define('store_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  # Course/user messages for composite partition tests
  CourseCreated = Sourced::CCC::Message.define('store_test.course.created') do
    attribute :course_name, String
  end

  UserRegistered = Sourced::CCC::Message.define('store_test.user.registered') do
    attribute :user_id, String
    attribute :name, String
  end

  UserJoinedCourse = Sourced::CCC::Message.define('store_test.user.joined_course') do
    attribute :course_name, String
    attribute :user_id, String
  end

  CourseClosed = Sourced::CCC::Message.define('store_test.course.closed') do
    attribute :course_name, String
  end
end

RSpec.describe Sourced::CCC::Store do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::CCC::Store.new(db) }

  before do
    store.install!
  end

  describe '#installed?' do
    it 'returns true after install!' do
      expect(store.installed?).to be true
    end

    it 'returns false before install!' do
      fresh_db = Sequel.sqlite
      fresh_store = Sourced::CCC::Store.new(fresh_db)
      expect(fresh_store.installed?).to be false
    end
  end

  describe '#install!' do
    it 'creates the three tables' do
      expect(db.table_exists?(:ccc_messages)).to be true
      expect(db.table_exists?(:ccc_key_pairs)).to be true
      expect(db.table_exists?(:ccc_message_key_pairs)).to be true
    end

    it 'is idempotent' do
      expect { store.install! }.not_to raise_error
    end
  end

  describe '#append' do
    it 'appends a single message and returns position' do
      msg = CCCStoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      )
      pos = store.append(msg)
      expect(pos).to eq(1)
    end

    it 'appends multiple messages and returns last position' do
      msgs = [
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }),
        CCCStoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' })
      ]
      pos = store.append(msgs)
      expect(pos).to eq(2)
    end

    it 'extracts and indexes key pairs' do
      msg = CCCStoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      )
      store.append(msg)

      key_pairs = db[:ccc_key_pairs].all
      expect(key_pairs.map { |r| [r[:name], r[:value]] }).to contain_exactly(
        ['device_id', 'dev-1'],
        ['name', 'Sensor A']
      )

      join_rows = db[:ccc_message_key_pairs].all
      expect(join_rows.size).to eq(2)
    end

    it 'deduplicates key pairs across messages' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      msg2 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor B' })
      store.append([msg1, msg2])

      # 'device_id'/'dev-1' should exist once in key_pairs
      count = db[:ccc_key_pairs].where(name: 'device_id', value: 'dev-1').count
      expect(count).to eq(1)

      # But both messages reference it via the join table
      kp_id = db[:ccc_key_pairs].where(name: 'device_id', value: 'dev-1').get(:id)
      join_count = db[:ccc_message_key_pairs].where(key_pair_id: kp_id).count
      expect(join_count).to eq(2)
    end

    it 'stores metadata as JSON' do
      msg = CCCStoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      store.append(msg)

      row = db[:ccc_messages].first
      meta = JSON.parse(row[:metadata], symbolize_names: true)
      expect(meta[:user_id]).to eq(42)
    end

    it 'persists and round-trips causation_id and correlation_id' do
      source = CCCStoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      )
      caused = source.correlate(
        CCCStoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' })
      )
      store.append([source, caused])

      cond1 = Sourced::CCC::QueryCondition.new(message_type: 'store_test.device.registered', key_name: 'device_id', key_value: 'dev-1')
      cond2 = Sourced::CCC::QueryCondition.new(message_type: 'store_test.asset.registered', key_name: 'asset_id', key_value: 'asset-1')
      messages, = store.read([cond1, cond2])

      src = messages.find { |m| m.type == 'store_test.device.registered' }
      csd = messages.find { |m| m.type == 'store_test.asset.registered' }

      expect(src.causation_id).to eq(src.id)
      expect(src.correlation_id).to eq(src.id)
      expect(csd.causation_id).to eq(source.id)
      expect(csd.correlation_id).to eq(source.correlation_id)
    end

    it 'returns latest_position for empty array' do
      pos = store.append([])
      expect(pos).to eq(0)
    end
  end

  describe '#append with guard (conditional append)' do
    let(:cond) do
      Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
    end

    it 'succeeds when no conflicts exist' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      _events, guard = store.read([cond])

      msg2 = CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      pos = store.append(msg2, guard: guard)
      expect(pos).to eq(2)
    end

    it 'raises ConcurrentAppendError when conflicts exist' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      _events, guard = store.read([cond])

      # Concurrent write by another process
      conflicting = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A v2' })
      store.append(conflicting)

      new_msg = CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect {
        store.append(new_msg, guard: guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'is atomic — failed append does not change store state' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      _events, guard = store.read([cond])

      # Concurrent write
      conflicting = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A v2' })
      store.append(conflicting)

      position_before = store.latest_position
      count_before = db[:ccc_messages].count

      new_msg = CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect {
        store.append(new_msg, guard: guard)
      }.to raise_error(Sourced::ConcurrentAppendError)

      expect(store.latest_position).to eq(position_before)
      expect(db[:ccc_messages].count).to eq(count_before)
    end

    it 'works with a manually constructed guard' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      guard = Sourced::CCC::ConsistencyGuard.new(conditions: [cond], last_position: store.latest_position)

      msg2 = CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      pos = store.append(msg2, guard: guard)
      expect(pos).to eq(2)
    end

    it 'append without guard still works unconditionally' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      msg2 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A v2' })
      pos = store.append(msg2)
      expect(pos).to eq(2)
    end
  end

  describe '#read' do
    before do
      store.append([
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }),
        CCCStoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' }),
        CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' }),
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'Sensor B' })
      ])
    end

    it 'queries by message_type and key condition' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      results, guard = store.read([cond])
      expect(results.size).to eq(1)
      expect(results.first.type).to eq('store_test.device.registered')
      expect(results.first.payload.device_id).to eq('dev-1')
    end

    it 'returns messages with position' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      results, _guard = store.read([cond])
      expect(results.first.position).to eq(1)
    end

    it 'queries with multiple OR conditions' do
      conditions = [
        Sourced::CCC::QueryCondition.new(
          message_type: 'store_test.device.registered',
          key_name: 'device_id',
          key_value: 'dev-1'
        ),
        Sourced::CCC::QueryCondition.new(
          message_type: 'store_test.device.bound',
          key_name: 'device_id',
          key_value: 'dev-1'
        )
      ]
      results, _guard = store.read(conditions)
      expect(results.size).to eq(2)
      expect(results.map(&:type)).to contain_exactly(
        'store_test.device.registered',
        'store_test.device.bound'
      )
    end

    it 'filters with from_position' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      # dev-1 registered is at position 1, so from_position: 1 should return nothing
      results, _guard = store.read([cond], from_position: 1)
      expect(results).to be_empty

      # from_position: 0 should return it
      results, _guard = store.read([cond], from_position: 0)
      expect(results.size).to eq(1)
    end

    it 'applies limit' do
      conditions = [
        Sourced::CCC::QueryCondition.new(
          message_type: 'store_test.device.registered',
          key_name: 'device_id',
          key_value: 'dev-1'
        ),
        Sourced::CCC::QueryCondition.new(
          message_type: 'store_test.device.bound',
          key_name: 'device_id',
          key_value: 'dev-1'
        )
      ]
      results, _guard = store.read(conditions, limit: 1)
      expect(results.size).to eq(1)
      expect(results.first.position).to eq(1) # first by position order
    end

    it 'returns empty for non-matching conditions' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'nonexistent'
      )
      results, _guard = store.read([cond])
      expect(results).to be_empty
    end

    it 'returns empty for empty conditions' do
      results, _guard = store.read([])
      expect(results).to be_empty
    end

    it 'deserializes into correct message subclasses' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      results, _guard = store.read([cond])
      msg = results.first
      expect(msg).to be_a(CCCStoreTestMessages::DeviceRegistered)
      expect(msg.id).to match(/\A[0-9a-f-]{36}\z/)
      expect(msg.created_at).to be_a(Time)
    end

    it 'returns a ConsistencyGuard with correct conditions' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      _results, guard = store.read([cond])
      expect(guard).to be_a(Sourced::CCC::ConsistencyGuard)
      expect(guard.conditions).to eq([cond])
    end

    it 'guard last_position reflects the last result position' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-2'
      )
      results, guard = store.read([cond])
      # dev-2 is at position 4
      expect(results.size).to eq(1)
      expect(guard.last_position).to eq(4)
    end

    it 'guard last_position falls back to latest_position when no results' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'nonexistent'
      )
      _results, guard = store.read([cond])
      expect(guard.last_position).to eq(store.latest_position)
    end

    it 'guard last_position falls back to from_position when no results and from_position given' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'nonexistent'
      )
      _results, guard = store.read([cond], from_position: 2)
      expect(guard.last_position).to eq(2)
    end
  end

  describe '#messages_since' do
    it 'returns messages after the given position' do
      msg1 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)
      pos = store.latest_position

      msg2 = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A Updated' })
      store.append(msg2)

      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )

      conflicts, guard = store.messages_since([cond], pos)
      expect(conflicts.size).to eq(1)
      expect(conflicts.first.payload.name).to eq('Sensor A Updated')
      expect(guard).to be_a(Sourced::CCC::ConsistencyGuard)
    end

    it 'returns empty when no new messages' do
      msg = CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg)
      pos = store.latest_position

      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )

      conflicts, _guard = store.messages_since([cond], pos)
      expect(conflicts).to be_empty
    end
  end

  describe '#latest_position' do
    it 'returns 0 for empty store' do
      expect(store.latest_position).to eq(0)
    end

    it 'returns max position after appends' do
      store.append([
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])
      expect(store.latest_position).to eq(2)
    end
  end

  describe '#clear!' do
    it 'deletes all data and resets position' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      expect(store.latest_position).to eq(1)

      store.clear!

      expect(store.latest_position).to eq(0)
      expect(db[:ccc_messages].count).to eq(0)
      expect(db[:ccc_key_pairs].count).to eq(0)
      expect(db[:ccc_message_key_pairs].count).to eq(0)
    end

    it 'resets autoincrement so next position starts from 1' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.clear!

      pos = store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      )
      expect(pos).to eq(1)
    end

    it 'clears consumer groups, offsets, and offset_key_pairs' do
      store.register_consumer_group('test-group')
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('test-group',
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1')

      store.clear!

      expect(db[:ccc_consumer_groups].count).to eq(0)
      expect(db[:ccc_offsets].count).to eq(0)
      expect(db[:ccc_offset_key_pairs].count).to eq(0)
    end
  end

  describe '#register_consumer_group' do
    it 'creates row with active status' do
      store.register_consumer_group('my-group')
      row = db[:ccc_consumer_groups].where(group_id: 'my-group').first
      expect(row).not_to be_nil
      expect(row[:status]).to eq('active')
      expect(row[:created_at]).not_to be_nil
      expect(row[:updated_at]).not_to be_nil
    end

    it 'is idempotent' do
      store.register_consumer_group('my-group')
      expect { store.register_consumer_group('my-group') }.not_to raise_error
      expect(db[:ccc_consumer_groups].where(group_id: 'my-group').count).to eq(1)
    end
  end

  describe '#consumer_group_active?' do
    it 'returns true for active group' do
      store.register_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be true
    end

    it 'returns false for stopped group' do
      store.register_consumer_group('my-group')
      store.stop_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be false
    end

    it 'returns false for nonexistent group' do
      expect(store.consumer_group_active?('nope')).to be false
    end
  end

  describe '#stop/start_consumer_group' do
    it 'toggles status' do
      store.register_consumer_group('my-group')
      store.stop_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be false

      store.start_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be true
    end
  end

  describe '#reset_consumer_group' do
    it 'deletes all offsets' do
      store.register_consumer_group('my-group')
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('my-group',
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1')

      expect(db[:ccc_offsets].count).to be > 0
      store.reset_consumer_group('my-group')
      expect(db[:ccc_offsets].count).to eq(0)
    end
  end

  describe '#claim_next (single attribute partition)' do
    let(:group_id) { 'single-test' }
    let(:handled_types) { ['store_test.device.registered'] }

    before do
      store.register_consumer_group(group_id)
    end

    it 'bootstraps offsets for new partitions' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(db[:ccc_offsets].count).to be >= 1
    end

    it 'returns nil when no pending messages' do
      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'returns messages for next unclaimed partition with correct shape' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).not_to be_nil
      expect(result[:offset_id]).to be_a(Integer)
      expect(result[:key_pair_ids]).to be_a(Array)
      expect(result[:partition_key]).to eq('device_id:dev-1')
      expect(result[:partition_value]).to eq({ 'device_id' => 'dev-1' })
      expect(result[:messages]).to be_a(Array)
      expect(result[:messages].size).to eq(1)
      expect(result[:messages].first).to be_a(CCCStoreTestMessages::DeviceRegistered)
      expect(result[:messages].first.position).to eq(1)
      expect(result[:guard]).to be_a(Sourced::CCC::ConsistencyGuard)
    end

    it 'returns multiple pending messages for same partition' do
      store.append([
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result[:messages].size).to eq(2)
    end

    it 'skips claimed partitions — second worker gets different partition' do
      store.append([
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-2')

      expect(r1[:partition_key]).not_to eq(r2[:partition_key])
    end

    it 'returns nil when all partitions claimed' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-2')
      expect(result).to be_nil
    end

    it 'respects handled_types filter' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: ['store_test.asset.registered'], worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'only returns messages after last_position' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1[:offset_id], position: r1[:messages].last.position)

      # Append another message for same partition
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A updated' })
      )

      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2[:messages].size).to eq(1)
      expect(r2[:messages].first.payload.name).to eq('A updated')
    end

    it 'returns nil for stopped consumer group' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.stop_consumer_group(group_id)

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'prioritizes partition with earliest pending message' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      )
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      # dev-2 was appended first (position 1), so it should be prioritized
      expect(result[:partition_value]).to eq({ 'device_id' => 'dev-2' })
    end

    it 'bootstraps newly appeared partitions on subsequent calls' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1[:offset_id], position: r1[:messages].last.position)

      # New partition appears
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-3', name: 'C' })
      )

      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2).not_to be_nil
      expect(r2[:partition_value]).to eq({ 'device_id' => 'dev-3' })
    end

    it 'returns a guard with conditions only for key_names each type actually has' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      guard = result[:guard]

      expect(guard).to be_a(Sourced::CCC::ConsistencyGuard)
      expect(guard.last_position).to eq(result[:messages].last.position)

      # 1 key_pair (device_id=dev-1) × 1 handled_type = 1 condition
      expect(guard.conditions.size).to eq(1)
      cond = guard.conditions.first
      expect(cond.message_type).to eq('store_test.device.registered')
      expect(cond.key_name).to eq('device_id')
      expect(cond.key_value).to eq('dev-1')
    end

    it 'guard can be used for optimistic concurrency on append' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')

      # No concurrent writes — append with guard succeeds
      new_event = CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect { store.append(new_event, guard: result[:guard]) }.not_to raise_error

      store.ack(group_id, offset_id: result[:offset_id], position: result[:messages].last.position)
    end

    it 'guard detects concurrent conflicting writes' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')

      # Simulate concurrent write after claim
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Concurrent' })
      )

      new_event = CCCStoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect {
        store.append(new_event, guard: result[:guard])
      }.to raise_error(Sourced::ConcurrentAppendError)
    end
  end

  describe '#claim_next (composite partition — conditional AND fetch)' do
    let(:group_id) { 'composite-test' }
    let(:handled_types) do
      [
        'store_test.course.created',
        'store_test.user.registered',
        'store_test.user.joined_course',
        'store_test.course.closed'
      ]
    end

    before do
      store.register_consumer_group(group_id)
    end

    it 'bootstraps composite partitions (only messages with ALL attributes create partitions)' do
      # CourseCreated only has course_name — not enough for a (course_name, user_id) partition
      store.append(
        CCCStoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' })
      )

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil

      # UserJoinedCourse has both — NOW a partition is created
      store.append(
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      )

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      expect(result).not_to be_nil
      expect(result[:partition_value]).to eq({ 'course_name' => 'Algebra', 'user_id' => 'joe' })
    end

    it 'fetches messages with single partition attribute matching' do
      store.append([
        CCCStoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        CCCStoreTestMessages::UserRegistered.new(payload: { user_id: 'joe', name: 'Joe' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      types = result[:messages].map(&:type)
      expect(types).to contain_exactly(
        'store_test.course.created',
        'store_test.user.registered',
        'store_test.user.joined_course'
      )
    end

    it 'different composite partitions can be claimed in parallel' do
      store.append([
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Physics', user_id: 'jake' })
      ])

      r1 = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      r2 = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-2')

      expect(r1).not_to be_nil
      expect(r2).not_to be_nil
      expect(r1[:partition_key]).not_to eq(r2[:partition_key])
    end

    it 'excludes messages with ALL partition attributes that do not match ALL values' do
      store.append([
        CCCStoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'jake' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')

      # The first partition claimed should be one of the two — let's check its messages
      if result[:partition_value]['user_id'] == 'joe'
        # Should include CourseCreated (1 attr, matches) and joe's join (2 attrs, both match)
        # Should NOT include jake's join (2 attrs, user_id doesn't match)
        user_ids = result[:messages]
          .select { |m| m.type == 'store_test.user.joined_course' }
          .map { |m| m.payload.user_id }
        expect(user_ids).to eq(['joe'])
      else
        user_ids = result[:messages]
          .select { |m| m.type == 'store_test.user.joined_course' }
          .map { |m| m.payload.user_id }
        expect(user_ids).to eq(['jake'])
      end
    end

    it 'messages with ALL partition attributes matching are not duplicated' do
      store.append([
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      # The join message matches both key_pairs but should appear only once
      expect(result[:messages].size).to eq(1)
    end

    it 'excludes messages with partial attributes matching wrong value' do
      store.append([
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'History', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')

      # Whichever partition we get, the other course's join should be excluded
      courses = result[:messages].map { |m| m.payload.course_name }
      expect(courses.uniq.size).to eq(1)
    end

    it 'returns guard with conditions only for key_names each message type actually has' do
      store.append([
        CCCStoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      guard = result[:guard]

      expect(guard.last_position).to eq(result[:messages].last.position)

      # Expected conditions (derived from message class definitions, not store data):
      #   CourseCreated     has course_name only     → 1 condition
      #   UserRegistered    has user_id (+ name, not a partition attr) → 1 condition
      #   CourseClosed      has course_name only     → 1 condition
      #   UserJoinedCourse  has course_name + user_id → 2 conditions
      # Total: 5 conditions
      expect(guard.conditions.size).to eq(5)

      # CourseCreated should NOT have a user_id condition
      course_created_conditions = guard.conditions.select { |c| c.message_type == 'store_test.course.created' }
      expect(course_created_conditions.size).to eq(1)
      expect(course_created_conditions.first.key_name).to eq('course_name')

      # UserRegistered should NOT have a course_name condition
      user_registered_conditions = guard.conditions.select { |c| c.message_type == 'store_test.user.registered' }
      expect(user_registered_conditions.size).to eq(1)
      expect(user_registered_conditions.first.key_name).to eq('user_id')

      # UserJoinedCourse has both
      joined_conditions = guard.conditions.select { |c| c.message_type == 'store_test.user.joined_course' }
      expect(joined_conditions.size).to eq(2)
      expect(joined_conditions.map(&:key_name).sort).to eq(['course_name', 'user_id'])
    end

    it 'guard detects concurrent writes in composite partition' do
      store.append([
        CCCStoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')

      # Concurrent write: course closed while decider was processing
      store.append(
        CCCStoreTestMessages::CourseClosed.new(payload: { course_name: 'Algebra' })
      )

      new_event = CCCStoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      expect {
        store.append(new_event, guard: result[:guard])
      }.to raise_error(Sourced::ConcurrentAppendError)
    end
  end

  describe '#ack' do
    let(:group_id) { 'ack-test' }

    before do
      store.register_consumer_group(group_id)
    end

    it 'advances offset and releases claim' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')

      store.ack(group_id, offset_id: result[:offset_id], position: result[:messages].last.position)

      offset = db[:ccc_offsets].where(id: result[:offset_id]).first
      expect(offset[:last_position]).to eq(result[:messages].last.position)
      expect(offset[:claimed]).to eq(0)
      expect(offset[:claimed_at]).to be_nil
      expect(offset[:claimed_by]).to be_nil
    end

    it 'after ack, subsequent claim skips processed messages' do
      store.append([
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      r1 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.ack(group_id, offset_id: r1[:offset_id], position: r1[:messages].last.position)

      # No new messages — should return nil
      r2 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      expect(r2).to be_nil
    end
  end

  describe '#release' do
    let(:group_id) { 'release-test' }

    before do
      store.register_consumer_group(group_id)
    end

    it 'releases claim without advancing' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')

      store.release(group_id, offset_id: result[:offset_id])

      offset = db[:ccc_offsets].where(id: result[:offset_id]).first
      expect(offset[:last_position]).to eq(0) # not advanced
      expect(offset[:claimed]).to eq(0)
    end

    it 'after release, same partition re-claimed with same messages' do
      store.append(
        CCCStoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.release(group_id, offset_id: r1[:offset_id])

      r2 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-2')

      expect(r2[:offset_id]).to eq(r1[:offset_id])
      expect(r2[:messages].map(&:position)).to eq(r1[:messages].map(&:position))
    end
  end
end
