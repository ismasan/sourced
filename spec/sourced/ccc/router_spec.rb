# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

module CCCRouterTestMessages
  DeviceRegistered = Sourced::CCC::Message.define('router_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  DeviceBound = Sourced::CCC::Message.define('router_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  BindDevice = Sourced::CCC::Message.define('router_test.bind_device') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  NotifyBound = Sourced::CCC::Message.define('router_test.notify_bound') do
    attribute :device_id, String
  end

  # Projector messages
  DeviceListed = Sourced::CCC::Message.define('router_test.device.listed') do
    attribute :device_id, String
  end
end

# Test decider for router specs
class RouterTestDecider < Sourced::CCC::Decider
  partition_by :device_id
  consumer_group 'router-test-decider'

  state { |_| { exists: false, bound: false } }

  evolve CCCRouterTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  evolve CCCRouterTestMessages::DeviceBound do |state, _evt|
    state[:bound] = true
  end

  command CCCRouterTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event CCCRouterTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end
end

# Test projector for router specs
class RouterTestProjector < Sourced::CCC::Projector
  partition_by :device_id
  consumer_group 'router-test-projector'

  state { |_| { devices: [] } }

  evolve CCCRouterTestMessages::DeviceRegistered do |state, evt|
    state[:devices] << evt.payload.name
  end

  evolve CCCRouterTestMessages::DeviceBound do |state, _evt|
    # nothing
  end

  sync do |state:, messages:, replaying:|
    # In a real projector, this would persist to DB
    state[:synced] = true
  end
end

RSpec.describe Sourced::CCC::Router do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::CCC::Store.new(db) }
  let(:router) { Sourced::CCC::Router.new(store: store) }

  before do
    store.install!
  end

  describe '#register' do
    it 'creates consumer group and introspects handle_batch signature' do
      router.register(RouterTestDecider)

      expect(store.consumer_group_active?('router-test-decider')).to be true
      expect(router.reactors).to include(RouterTestDecider)
    end

    it 'detects history: for decider, none for projector' do
      router.register(RouterTestDecider)
      router.register(RouterTestProjector)

      # Decider needs history, projector does not
      expect(router.instance_variable_get(:@needs_history)[RouterTestDecider]).to be true
      expect(router.instance_variable_get(:@needs_history)[RouterTestProjector]).to be false
    end
  end

  describe '#handle_next_for' do
    before do
      router.register(RouterTestDecider)
      router.register(RouterTestProjector)
    end

    it 'returns false when no work available' do
      result = router.handle_next_for(RouterTestDecider)
      expect(result).to be false
    end

    it 'claims, calls handle_batch, executes actions + acks in transaction' do
      # Set up: register device first (as history), then send bind command
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      result = router.handle_next_for(RouterTestDecider)
      expect(result).to be true

      # DeviceBound event should have been appended to store
      conds = CCCRouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      read_result = store.read(conds)
      expect(read_result.messages.size).to eq(1)
      expect(read_result.messages.first).to be_a(CCCRouterTestMessages::DeviceBound)
    end

    it 'reads history for decider, skips history for projector' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )

      # Projector should process without history
      result = router.handle_next_for(RouterTestProjector)
      expect(result).to be true
    end

    it 'releases on ConcurrentAppendError' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      # Stub store.append to raise ConcurrentAppendError after claim
      original_append = store.method(:append)
      call_count = 0
      allow(store).to receive(:append) do |*args, **kwargs|
        call_count += 1
        if call_count > 0 && kwargs[:guard]
          raise Sourced::ConcurrentAppendError, 'conflict'
        end
        original_append.call(*args, **kwargs)
      end

      result = router.handle_next_for(RouterTestDecider)
      expect(result).to be true

      # Offset should be released (not advanced) — can re-claim
      claim = store.claim_next(
        'router-test-decider',
        partition_by: ['device_id'],
        handled_types: RouterTestDecider.handled_messages.map(&:type),
        worker_id: 'w-1'
      )
      expect(claim).not_to be_nil
    end

    it 'releases on StandardError and calls on_exception' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      # Make handle_batch raise
      allow(RouterTestDecider).to receive(:handle_batch).and_raise(RuntimeError, 'boom')
      allow(RouterTestDecider).to receive(:on_exception)

      result = router.handle_next_for(RouterTestDecider)
      expect(result).to be true
      expect(RouterTestDecider).to have_received(:on_exception)
    end
  end

  describe '#drain' do
    before do
      router.register(RouterTestDecider)
      router.register(RouterTestProjector)
    end

    it 'processes all reactors until no work remains' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      router.drain

      # Decider should have produced DeviceBound
      conds = CCCRouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      read_result = store.read(conds)
      expect(read_result.messages.size).to eq(1)

      # Projector should have processed DeviceRegistered and DeviceBound
      # (it handles both via evolve)
    end
  end

  describe 'full integration: append commands → Decider → events → Projector' do
    before do
      router.register(RouterTestDecider)
      router.register(RouterTestProjector)
    end

    it 'end-to-end flow works' do
      # 1. Register device (event — goes to projector directly)
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )

      # 2. Send bind command (decider will produce DeviceBound event)
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      # 3. Drain — decider processes command, projector processes events
      router.drain

      # 4. Verify DeviceBound was appended
      conds = CCCRouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      read_result = store.read(conds)
      bound_events = read_result.messages.select { |m| m.is_a?(CCCRouterTestMessages::DeviceBound) }
      expect(bound_events.size).to eq(1)

      # Verify causation chain
      bound = bound_events.first
      expect(bound.causation_id).not_to be_nil
      expect(bound.correlation_id).not_to be_nil
    end
  end
end
