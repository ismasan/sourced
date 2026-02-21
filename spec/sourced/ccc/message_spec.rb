# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

module CCCTestMessages
  DeviceRegistered = Sourced::CCC::Message.define('device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  AssetRegistered = Sourced::CCC::Message.define('asset.registered') do
    attribute :asset_id, String
    attribute :label, String
  end

  SystemUpdated = Sourced::CCC::Message.define('system.updated') do
    attribute :version, String
  end

  OptionalFields = Sourced::CCC::Message.define('test.optional_fields') do
    attribute? :required_field, String
    attribute? :optional_field, String
  end
end

RSpec.describe Sourced::CCC::Message do
  describe '.define' do
    it 'creates a subclass with a type string' do
      expect(CCCTestMessages::DeviceRegistered.type).to eq('device.registered')
    end

    it 'creates a typed payload' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.payload.device_id).to eq('dev-1')
      expect(msg.payload.name).to eq('Sensor A')
    end

    it 'auto-generates an id' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.id).not_to be_nil
      expect(msg.id).to match(/\A[0-9a-f-]{36}\z/)
    end

    it 'sets created_at automatically' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.created_at).to be_a(Time)
    end

    it 'sets type on the instance' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.type).to eq('device.registered')
    end

    it 'defaults metadata to empty hash' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.metadata).to eq({})
    end

    it 'accepts metadata' do
      msg = CCCTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      expect(msg.metadata[:user_id]).to eq(42)
    end

    it 'defaults causation_id and correlation_id to id' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.causation_id).to eq(msg.id)
      expect(msg.correlation_id).to eq(msg.id)
    end

    it 'accepts explicit causation_id and correlation_id' do
      msg = CCCTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        causation_id: 'cause-1',
        correlation_id: 'corr-1'
      )
      expect(msg.causation_id).to eq('cause-1')
      expect(msg.correlation_id).to eq('corr-1')
    end
  end

  describe '.from' do
    it 'instantiates the correct subclass from a hash' do
      msg = Sourced::CCC::Message.from(type: 'device.registered', payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg).to be_a(CCCTestMessages::DeviceRegistered)
      expect(msg.payload.device_id).to eq('dev-1')
    end

    it 'raises UnknownMessageError for unknown types' do
      expect {
        Sourced::CCC::Message.from(type: 'unknown.type', payload: {})
      }.to raise_error(Sourced::UnknownMessageError, /Unknown message type: unknown.type/)
    end
  end

  describe '.registry' do
    it 'stores defined message types' do
      expect(Sourced::CCC::Message.registry['device.registered']).to eq(CCCTestMessages::DeviceRegistered)
      expect(Sourced::CCC::Message.registry['asset.registered']).to eq(CCCTestMessages::AssetRegistered)
    end

    it 'is separate from Sourced::Message registry' do
      expect(Sourced::CCC::Message.registry['device.registered']).not_to be_nil
      expect(Sourced::Message.registry['device.registered']).to be_nil
    end
  end

  describe '#extracted_keys' do
    it 'extracts all top-level payload attributes as string pairs' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      keys = msg.extracted_keys
      expect(keys).to contain_exactly(
        ['device_id', 'dev-1'],
        ['name', 'Sensor A']
      )
    end

    it 'skips nil values' do
      msg = CCCTestMessages::OptionalFields.new(payload: { required_field: 'present', optional_field: nil })
      keys = msg.extracted_keys
      expect(keys).to eq([['required_field', 'present']])
    end

    it 'converts values to strings' do
      msg = CCCTestMessages::SystemUpdated.new(payload: { version: 'v2.0.5' })
      keys = msg.extracted_keys
      expect(keys).to eq([['version', 'v2.0.5']])
    end

    it 'returns empty array for messages without payload attributes' do
      # Message base class with no payload definition
      bare = Sourced::CCC::Message.define('test.bare')
      msg = bare.new
      expect(msg.extracted_keys).to eq([])
    end
  end

  describe '.payload_attribute_names' do
    it 'returns attribute names for a defined message class' do
      expect(CCCTestMessages::DeviceRegistered.payload_attribute_names).to eq([:device_id, :name])
    end

    it 'returns empty array for a bare message class' do
      bare = Sourced::CCC::Message.define('test.payload_attrs.bare')
      expect(bare.payload_attribute_names).to eq([])
    end
  end

  describe '.to_conditions' do
    it 'returns conditions only for attributes the message class has' do
      conditions = CCCTestMessages::DeviceRegistered.to_conditions(device_id: 'dev-1', asset_id: 'asset-1')
      expect(conditions.size).to eq(1)
      expect(conditions.first.message_type).to eq('device.registered')
      expect(conditions.first.key_name).to eq('device_id')
      expect(conditions.first.key_value).to eq('dev-1')
    end

    it 'returns conditions for all matching attributes' do
      conditions = CCCTestMessages::DeviceRegistered.to_conditions(device_id: 'dev-1', name: 'Sensor A')
      expect(conditions.size).to eq(2)
      expect(conditions.map(&:key_name).sort).to eq(['device_id', 'name'])
    end

    it 'returns empty array when no attributes match' do
      conditions = CCCTestMessages::DeviceRegistered.to_conditions(course_name: 'Algebra')
      expect(conditions).to eq([])
    end
  end

  describe '#correlate' do
    it 'sets causation_id to source message id' do
      source = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      target = CCCTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Label' })

      correlated = source.correlate(target)
      expect(correlated.causation_id).to eq(source.id)
    end

    it 'propagates correlation_id from source' do
      source = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      target = CCCTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Label' })

      correlated = source.correlate(target)
      expect(correlated.correlation_id).to eq(source.correlation_id)
    end

    it 'preserves correlation_id through a chain' do
      first = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      second = first.correlate(CCCTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Label' }))
      third = second.correlate(CCCTestMessages::SystemUpdated.new(payload: { version: 'v1' }))

      expect(third.causation_id).to eq(second.id)
      expect(third.correlation_id).to eq(first.id)
    end

    it 'merges metadata from both messages' do
      source = CCCTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      target = CCCTestMessages::AssetRegistered.new(
        payload: { asset_id: 'asset-1', label: 'Label' },
        metadata: { request_id: 'req-1' }
      )

      correlated = source.correlate(target)
      expect(correlated.metadata).to eq({ user_id: 42, request_id: 'req-1' })
    end

    it 'returns a new instance without mutating the original' do
      source = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      target = CCCTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Label' })

      correlated = source.correlate(target)
      expect(correlated).not_to equal(target)
      expect(correlated).to be_a(CCCTestMessages::AssetRegistered)
      expect(target.causation_id).to eq(target.id) # original unchanged
    end
  end

  describe Sourced::CCC::QueryCondition do
    it 'is a Data struct with message_type, key_name, key_value' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'device.registered',
        key_name: 'device_id',
        key_value: 'dev-1'
      )
      expect(cond.message_type).to eq('device.registered')
      expect(cond.key_name).to eq('device_id')
      expect(cond.key_value).to eq('dev-1')
    end
  end
end
