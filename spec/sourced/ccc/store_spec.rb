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

    it 'returns latest_position for empty array' do
      pos = store.append([])
      expect(pos).to eq(0)
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
      results = store.read([cond])
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
      results = store.read([cond])
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
      results = store.read(conditions)
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
      results = store.read([cond], from_position: 1)
      expect(results).to be_empty

      # from_position: 0 should return it
      results = store.read([cond], from_position: 0)
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
      results = store.read(conditions, limit: 1)
      expect(results.size).to eq(1)
      expect(results.first.position).to eq(1) # first by position order
    end

    it 'returns empty for non-matching conditions' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'nonexistent'
      )
      results = store.read([cond])
      expect(results).to be_empty
    end

    it 'returns empty for empty conditions' do
      expect(store.read([])).to be_empty
    end

    it 'deserializes into correct message subclasses' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'store_test.device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      results = store.read([cond])
      msg = results.first
      expect(msg).to be_a(CCCStoreTestMessages::DeviceRegistered)
      expect(msg.id).to match(/\A[0-9a-f-]{36}\z/)
      expect(msg.created_at).to be_a(Time)
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

      conflicts = store.messages_since([cond], pos)
      expect(conflicts.size).to eq(1)
      expect(conflicts.first.payload.name).to eq('Sensor A Updated')
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

      conflicts = store.messages_since([cond], pos)
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
  end
end
