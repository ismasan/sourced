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

  # Simple reactor messages
  DeviceAudited = Sourced::CCC::Message.define('router_test.device.audited') do
    attribute :device_id, String
    attribute :event_type, String
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

  reaction CCCRouterTestMessages::DeviceBound do |_state, evt|
    CCCRouterTestMessages::NotifyBound.new(payload: { device_id: evt.payload.device_id })
  end
end

# Test projector for router specs
class RouterTestProjector < Sourced::CCC::Projector::StateStored
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

# Simple reactor: just extends Consumer, defines handled_messages, implements handle_batch.
# Logs an audit trail message for every DeviceRegistered or DeviceBound it sees.
class RouterTestAuditReactor
  extend Sourced::CCC::Consumer

  partition_by :device_id
  consumer_group 'router-test-audit'

  def self.handled_messages
    [CCCRouterTestMessages::DeviceRegistered, CCCRouterTestMessages::DeviceBound]
  end

  def self.handle_batch(claim)
    each_with_partial_ack(claim.messages) do |msg|
      audit = CCCRouterTestMessages::DeviceAudited.new(
        payload: { device_id: msg.payload.device_id, event_type: msg.type }
      )
      [Sourced::CCC::Actions::Append.new(audit), msg]
    end
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

    it 'on_exception stops consumer group when default strategy' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      allow(RouterTestDecider).to receive(:handle_batch).and_raise(RuntimeError, 'boom')

      router.handle_next_for(RouterTestDecider)
      expect(store.consumer_group_active?(RouterTestDecider.group_id)).to be false
    end

    it 'on_exception persists error_context in the database' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      allow(RouterTestDecider).to receive(:handle_batch).and_raise(RuntimeError, 'boom')

      router.handle_next_for(RouterTestDecider)

      row = db[:ccc_consumer_groups].where(group_id: RouterTestDecider.group_id).first
      expect(row[:error_context]).not_to be_nil
      expect(row[:status]).to eq('stopped')
    end

    it 'on_exception with retry strategy sets retry_at on consumer group' do
      store.append(
        CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      retry_strategy = Sourced::ErrorStrategy.new do |s|
        s.retry(times: 3, after: 5)
      end
      allow(Sourced::CCC).to receive_message_chain(:config, :error_strategy).and_return(retry_strategy)

      allow(RouterTestDecider).to receive(:handle_batch).and_raise(RuntimeError, 'boom')

      router.handle_next_for(RouterTestDecider)

      row = db[:ccc_consumer_groups].where(group_id: RouterTestDecider.group_id).first
      expect(row[:retry_at]).not_to be_nil
      expect(row[:status]).to eq('active')

      ctx = JSON.parse(row[:error_context], symbolize_names: true)
      expect(ctx[:retry_count]).to eq(2)
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

    it 'events are correlated with the command that produced them' do
      reg = CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      cmd = CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      store.append(cmd)

      router.drain

      # Read the DeviceBound event
      conds = CCCRouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      bound = store.read(conds).messages.find { |m| m.is_a?(CCCRouterTestMessages::DeviceBound) }

      expect(bound).not_to be_nil
      expect(bound.causation_id).to eq(cmd.id)
      expect(bound.correlation_id).to eq(cmd.correlation_id)
    end

    it 'reaction messages are correlated with the event reacted to, not the command' do
      reg = CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      cmd = CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      store.append(cmd)

      router.drain

      # Read the DeviceBound event (produced by command handler)
      bound_conds = CCCRouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      bound = store.read(bound_conds).messages.find { |m| m.is_a?(CCCRouterTestMessages::DeviceBound) }

      # Read the NotifyBound reaction message (produced by reaction handler)
      notify_conds = CCCRouterTestMessages::NotifyBound.to_conditions(device_id: 'd1')
      notify = store.read(notify_conds).messages.find { |m| m.is_a?(CCCRouterTestMessages::NotifyBound) }

      expect(notify).not_to be_nil
      # Reaction is correlated with the event, not the command
      expect(notify.causation_id).to eq(bound.id)
      expect(notify.correlation_id).to eq(bound.correlation_id)
      # The whole chain shares the same correlation_id (the command's)
      expect(notify.correlation_id).to eq(cmd.correlation_id)
    end
  end

  describe 'simple Consumer reactor (no Decider/Projector)' do
    before do
      router.register(RouterTestAuditReactor)
    end

    it 'registers and processes messages through the router' do
      reg = CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      router.drain

      # Audit message should have been appended
      conds = CCCRouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      audits = store.read(conds).messages
      expect(audits.size).to eq(1)
      expect(audits.first).to be_a(CCCRouterTestMessages::DeviceAudited)
      expect(audits.first.payload.event_type).to eq('router_test.device.registered')
    end

    it 'appended messages are correlated with the source message' do
      reg = CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      router.drain

      conds = CCCRouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      audit = store.read(conds).messages.first

      expect(audit.causation_id).to eq(reg.id)
      expect(audit.correlation_id).to eq(reg.correlation_id)
    end

    it 'handles multiple messages across partitions' do
      store.append(CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor A' }))
      store.append(CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd2', name: 'Sensor B' }))

      router.drain

      d1_conds = CCCRouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      d2_conds = CCCRouterTestMessages::DeviceAudited.to_conditions(device_id: 'd2')

      expect(store.read(d1_conds).messages.size).to eq(1)
      expect(store.read(d2_conds).messages.size).to eq(1)
    end

    it 'context_for returns empty conditions (no evolve types)' do
      expect(RouterTestAuditReactor.context_for(device_id: 'd1')).to eq([])
    end

    it 'works alongside deciders and projectors' do
      router.register(RouterTestDecider)

      reg = CCCRouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      cmd = CCCRouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      store.append(cmd)

      router.drain

      # Audit reactor sees DeviceRegistered and DeviceBound
      conds = CCCRouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      audits = store.read(conds).messages
      types = audits.map { |m| m.payload.event_type }.sort

      expect(types).to eq([
        'router_test.device.bound',
        'router_test.device.registered'
      ])
    end
  end
end
