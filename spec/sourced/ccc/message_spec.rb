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
