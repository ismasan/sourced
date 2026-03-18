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
    it 'returns one condition with only attributes the message class has' do
      conditions = CCCTestMessages::DeviceRegistered.to_conditions(device_id: 'dev-1', asset_id: 'asset-1')
      expect(conditions.size).to eq(1)
      expect(conditions.first.message_type).to eq('device.registered')
      expect(conditions.first.attrs).to eq({ device_id: 'dev-1' })
    end

    it 'includes all matching attributes in one condition' do
      conditions = CCCTestMessages::DeviceRegistered.to_conditions(device_id: 'dev-1', name: 'Sensor A')
      expect(conditions.size).to eq(1)
      expect(conditions.first.attrs).to eq({ device_id: 'dev-1', name: 'Sensor A' })
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

  describe '#with_metadata' do
    it 'merges new metadata into existing metadata' do
      msg = CCCTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      updated = msg.with_metadata(request_id: 'req-1')
      expect(updated.metadata).to eq({ user_id: 42, request_id: 'req-1' })
    end

    it 'returns self when given empty hash' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      expect(msg.with_metadata({})).to equal(msg)
    end

    it 'does not mutate the original message' do
      msg = CCCTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      updated = msg.with_metadata(request_id: 'req-1')
      expect(msg.metadata).to eq({ user_id: 42 })
      expect(updated).not_to equal(msg)
    end
  end

  describe '#with_payload' do
    it 'merges new attributes into existing payload' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      updated = msg.with_payload(name: 'Sensor B')
      expect(updated.payload.device_id).to eq('dev-1')
      expect(updated.payload.name).to eq('Sensor B')
    end

    it 'preserves id and other attributes' do
      msg = CCCTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      updated = msg.with_payload(name: 'Sensor B')
      expect(updated.id).to eq(msg.id)
      expect(updated.metadata).to eq({ user_id: 42 })
      expect(updated.type).to eq('device.registered')
    end

    it 'returns a new instance without mutating the original' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      updated = msg.with_payload(name: 'Sensor B')
      expect(updated).not_to equal(msg)
      expect(msg.payload.name).to eq('Sensor A')
    end

    it 'works with empty hash (returns equivalent copy)' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      updated = msg.with_payload({})
      expect(updated.payload.device_id).to eq('dev-1')
      expect(updated.payload.name).to eq('Sensor A')
      expect(updated).not_to equal(msg)
    end
  end

  describe '#at' do
    it 'returns new message with updated created_at' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      future = msg.created_at + 3600
      updated = msg.at(future)
      expect(updated.created_at).to eq(future)
      expect(updated).not_to equal(msg)
    end

    it 'raises PastMessageDateError when given a past time' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      past = msg.created_at - 3600
      expect { msg.at(past) }.to raise_error(Sourced::PastMessageDateError)
    end

    it 'does not mutate the original message' do
      msg = CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      original_time = msg.created_at
      msg.at(msg.created_at + 3600)
      expect(msg.created_at).to eq(original_time)
    end
  end

  describe 'Registry' do
    describe '#keys' do
      it 'returns array of registered type strings' do
        keys = Sourced::CCC::Message.registry.keys
        expect(keys).to include('device.registered', 'asset.registered', 'system.updated')
      end
    end

    describe '#all' do
      let!(:test_cmd) { Sourced::CCC::Command.define('test.reg_all_cmd') { attribute :name, String } }
      let!(:test_evt) { Sourced::CCC::Event.define('test.reg_all_evt') { attribute :name, String } }

      it 'returns an Enumerator when no block given' do
        expect(Sourced::CCC::Message.registry.all).to be_a(Enumerator)
      end

      it 'includes classes from subclass registries' do
        all = Sourced::CCC::Message.registry.all.to_a
        expect(all).to include(test_cmd)
        expect(all).to include(test_evt)
      end

      it 'includes classes registered directly on Message' do
        all = Sourced::CCC::Message.registry.all.to_a
        expect(all).to include(CCCTestMessages::DeviceRegistered)
      end

      it 'yields each class when block given' do
        yielded = []
        Sourced::CCC::Message.registry.all { |c| yielded << c }
        expect(yielded).to include(test_cmd, test_evt)
      end

      it 'scoped to a subclass registry only includes that branch' do
        cmd_all = Sourced::CCC::Command.registry.all.to_a
        expect(cmd_all).to include(test_cmd)
        expect(cmd_all).not_to include(test_evt)
      end
    end
  end

  describe 'Payload' do
    let(:msg) { CCCTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }) }

    describe '#[]' do
      it 'returns attribute value by symbol key' do
        expect(msg.payload[:device_id]).to eq('dev-1')
        expect(msg.payload[:name]).to eq('Sensor A')
      end
    end

    describe '#fetch' do
      it 'returns attribute value for existing key' do
        expect(msg.payload.fetch(:device_id)).to eq('dev-1')
      end

      it 'raises KeyError for missing key' do
        expect { msg.payload.fetch(:missing) }.to raise_error(KeyError)
      end

      it 'supports default value' do
        expect(msg.payload.fetch(:missing, 'default')).to eq('default')
      end

      it 'supports block fallback' do
        expect(msg.payload.fetch(:missing) { 'from_block' }).to eq('from_block')
      end
    end
  end

  describe '#initialize default payload' do
    it 'creates message without explicit payload arg' do
      bare = Sourced::CCC::Message.define('test.init_default')
      msg = bare.new
      expect(msg.payload).to be_nil
      expect(msg.id).not_to be_nil
      expect(msg.type).to eq('test.init_default')
    end
  end

  describe Sourced::CCC::ConsistencyGuard do
    it 'is a Data struct with conditions and last_position' do
      conditions = [Sourced::CCC::QueryCondition.new(message_type: 'device.registered', attrs: { device_id: 'dev-1' })]
      guard = Sourced::CCC::ConsistencyGuard.new(conditions: conditions, last_position: 42)
      expect(guard.conditions).to eq(conditions)
      expect(guard.last_position).to eq(42)
    end
  end

  describe 'Command and Event subclass registries' do
    let!(:test_cmd) { Sourced::CCC::Command.define('test.do_something') { attribute :name, String } }
    let!(:test_evt) { Sourced::CCC::Event.define('test.something_happened') { attribute :name, String } }

    it 'registers Command types in Command registry' do
      expect(Sourced::CCC::Command.registry['test.do_something']).to eq(test_cmd)
    end

    it 'registers Event types in Event registry' do
      expect(Sourced::CCC::Event.registry['test.something_happened']).to eq(test_evt)
    end

    it 'does not register Command types in Event registry' do
      expect(Sourced::CCC::Event.registry['test.do_something']).to be_nil
    end

    it 'Message.registry can look up types from subclass registries' do
      expect(Sourced::CCC::Message.registry['test.do_something']).to eq(test_cmd)
      expect(Sourced::CCC::Message.registry['test.something_happened']).to eq(test_evt)
    end
  end

  describe Sourced::CCC::QueryCondition do
    it 'is a Data struct with message_type and attrs hash' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'device.registered',
        attrs: { device_id: 'dev-1' }
      )
      expect(cond.message_type).to eq('device.registered')
      expect(cond.attrs).to eq({ device_id: 'dev-1' })
    end

    it 'supports multiple attrs for compound conditions' do
      cond = Sourced::CCC::QueryCondition.new(
        message_type: 'seat.selected',
        attrs: { showing_id: 'show-1', seat_id: 'C7' }
      )
      expect(cond.attrs).to eq({ showing_id: 'show-1', seat_id: 'C7' })
    end
  end
end
