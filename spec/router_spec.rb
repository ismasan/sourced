# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/store'
require 'sequel'

module RouterTestMessages
  DeviceRegistered = Sourced::Message.define('router_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  DeviceBound = Sourced::Message.define('router_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  BindDevice = Sourced::Message.define('router_test.bind_device') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  NotifyBound = Sourced::Message.define('router_test.notify_bound') do
    attribute :device_id, String
  end

  # Projector messages
  DeviceListed = Sourced::Message.define('router_test.device.listed') do
    attribute :device_id, String
  end

  # Simple reactor messages
  DeviceAudited = Sourced::Message.define('router_test.device.audited') do
    attribute :device_id, String
    attribute :event_type, String
  end
end

# Test decider for router specs
class RouterTestDecider < Sourced::Decider
  partition_by :device_id
  consumer_group 'router-test-decider'

  state { |_| { exists: false, bound: false } }

  evolve RouterTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  evolve RouterTestMessages::DeviceBound do |state, _evt|
    state[:bound] = true
  end

  command RouterTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event RouterTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end

  reaction RouterTestMessages::DeviceBound do |_state, evt|
    RouterTestMessages::NotifyBound.new(payload: { device_id: evt.payload.device_id })
  end
end

# Test projector for router specs
class RouterTestProjector < Sourced::Projector::StateStored
  partition_by :device_id
  consumer_group 'router-test-projector'

  state { |_| { devices: [] } }

  evolve RouterTestMessages::DeviceRegistered do |state, evt|
    state[:devices] << evt.payload.name
  end

  evolve RouterTestMessages::DeviceBound do |state, _evt|
    # nothing
  end

  sync do |state:, messages:, replaying:|
    # In a real projector, this would persist to DB
    state[:synced] = true
  end
end

# Simple reactor: just extends Consumer, defines handled_messages, implements handle_claim.
# Logs an audit trail message for every DeviceRegistered or DeviceBound it sees.
class RouterTestAuditReactor
  extend Sourced::Consumer

  partition_by :device_id
  consumer_group 'router-test-audit'

  def self.handled_messages
    [RouterTestMessages::DeviceRegistered, RouterTestMessages::DeviceBound]
  end

  def self.handle_claim(claim)
    each_with_partial_ack(claim.messages) do |msg|
      audit = RouterTestMessages::DeviceAudited.new(
        payload: { device_id: msg.payload.device_id, event_type: msg.type }
      )
      [Sourced::Actions::Append.new(audit), msg]
    end
  end
end

RSpec.describe Sourced::Router do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::Store.new(db) }
  let(:router) { Sourced::Router.new(store: store) }

  before do
    store.install!
  end

  describe '#register' do
    it 'creates consumer group and introspects handle_claim signature' do
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

    it 'passes partition_keys to register_consumer_group' do
      router.register(RouterTestDecider)

      row = db[:sourced_consumer_groups].where(group_id: 'router-test-decider').first
      expect(JSON.parse(row[:partition_by])).to eq(['device_id'])
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

    it 'claims, calls handle_claim, executes actions + acks in transaction' do
      # Set up: register device first (as history), then send bind command
      store.append(
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      result = router.handle_next_for(RouterTestDecider)
      expect(result).to be true

      # DeviceBound event should have been appended to store
      conds = RouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      read_result = store.read(conds)
      expect(read_result.messages.size).to eq(1)
      expect(read_result.messages.first).to be_a(RouterTestMessages::DeviceBound)
    end

    it 'reads history for decider, skips history for projector' do
      store.append(
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )

      # Projector should process without history
      result = router.handle_next_for(RouterTestProjector)
      expect(result).to be true
    end

    it 'releases on ConcurrentAppendError' do
      store.append(
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
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
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      # Make handle_claim raise
      allow(RouterTestDecider).to receive(:handle_claim).and_raise(RuntimeError, 'boom')
      allow(RouterTestDecider).to receive(:on_exception)

      result = router.handle_next_for(RouterTestDecider)
      expect(result).to be true
      expect(RouterTestDecider).to have_received(:on_exception)
    end

    it 'on_exception fails consumer group when default strategy' do
      store.append(
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      allow(RouterTestDecider).to receive(:handle_claim).and_raise(RuntimeError, 'boom')

      router.handle_next_for(RouterTestDecider)
      expect(store.consumer_group_active?(RouterTestDecider.group_id)).to be false
      row = db[:sourced_consumer_groups].where(group_id: RouterTestDecider.group_id).first
      expect(row[:status]).to eq('failed')
    end

    it 'on_exception persists error_context in the database' do
      store.append(
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      allow(RouterTestDecider).to receive(:handle_claim).and_raise(RuntimeError, 'boom')

      router.handle_next_for(RouterTestDecider)

      row = db[:sourced_consumer_groups].where(group_id: RouterTestDecider.group_id).first
      expect(row[:error_context]).not_to be_nil
      expect(row[:status]).to eq('failed')
    end

    it 'on_exception with retry strategy sets retry_at on consumer group' do
      store.append(
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      retry_strategy = Sourced::ErrorStrategy.new do |s|
        s.retry(times: 3, after: 5)
      end
      allow(Sourced).to receive_message_chain(:config, :error_strategy).and_return(retry_strategy)

      allow(RouterTestDecider).to receive(:handle_claim).and_raise(RuntimeError, 'boom')

      router.handle_next_for(RouterTestDecider)

      row = db[:sourced_consumer_groups].where(group_id: RouterTestDecider.group_id).first
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
        RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
      store.append(
        RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      )

      router.drain

      # Decider should have produced DeviceBound
      conds = RouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
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
      reg = RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      cmd = RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      store.append(cmd)

      router.drain

      # Read the DeviceBound event
      conds = RouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      bound = store.read(conds).messages.find { |m| m.is_a?(RouterTestMessages::DeviceBound) }

      expect(bound).not_to be_nil
      expect(bound.causation_id).to eq(cmd.id)
      expect(bound.correlation_id).to eq(cmd.correlation_id)
    end

    it 'reaction messages are correlated with the event reacted to, not the command' do
      reg = RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      cmd = RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      store.append(cmd)

      router.drain

      # Read the DeviceBound event (produced by command handler)
      bound_conds = RouterTestMessages::DeviceBound.to_conditions(device_id: 'd1')
      bound = store.read(bound_conds).messages.find { |m| m.is_a?(RouterTestMessages::DeviceBound) }

      # Read the NotifyBound reaction message (produced by reaction handler)
      notify_conds = RouterTestMessages::NotifyBound.to_conditions(device_id: 'd1')
      notify = store.read(notify_conds).messages.find { |m| m.is_a?(RouterTestMessages::NotifyBound) }

      expect(notify).not_to be_nil
      # Reaction is correlated with the event, not the command
      expect(notify.causation_id).to eq(bound.id)
      expect(notify.correlation_id).to eq(bound.correlation_id)
      # The whole chain shares the same correlation_id (the command's)
      expect(notify.correlation_id).to eq(cmd.correlation_id)
    end
  end

  describe 'consumer group lifecycle' do
    before do
      router.register(RouterTestDecider)
      router.register(RouterTestProjector)
    end

    describe '#stop_consumer_group' do
      it 'stops group and calls on_stop with class' do
        expect(store.consumer_group_active?(RouterTestDecider.group_id)).to be true

        router.stop_consumer_group(RouterTestDecider, 'maintenance')

        expect(store.consumer_group_active?(RouterTestDecider.group_id)).to be false
      end

      it 'stops group and calls on_stop with string group_id' do
        router.stop_consumer_group('router-test-decider', 'maintenance')

        expect(store.consumer_group_active?('router-test-decider')).to be false
      end

      it 'invokes on_stop callback on reactor class' do
        allow(RouterTestDecider).to receive(:on_stop)

        router.stop_consumer_group(RouterTestDecider, 'going down')

        expect(RouterTestDecider).to have_received(:on_stop).with('going down')
      end

      it 'passes nil message when none given' do
        allow(RouterTestDecider).to receive(:on_stop)

        router.stop_consumer_group(RouterTestDecider)

        expect(RouterTestDecider).to have_received(:on_stop).with(nil)
      end
    end

    describe '#reset_consumer_group' do
      it 'resets group offsets and calls on_reset with class' do
        # Append a message and drain so offsets are advanced
        store.append(
          RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
        )
        router.drain

        router.reset_consumer_group(RouterTestDecider)

        # Offsets should be cleared (group can re-process messages)
        row = db[:sourced_consumer_groups].where(group_id: RouterTestDecider.group_id).first
        expect(row[:discovery_position]).to eq(0)
      end

      it 'resets group with string group_id' do
        store.append(
          RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
        )
        router.drain

        router.reset_consumer_group('router-test-decider')

        row = db[:sourced_consumer_groups].where(group_id: 'router-test-decider').first
        expect(row[:discovery_position]).to eq(0)
      end

      it 'invokes on_reset callback on reactor class' do
        allow(RouterTestDecider).to receive(:on_reset)

        router.reset_consumer_group(RouterTestDecider)

        expect(RouterTestDecider).to have_received(:on_reset)
      end
    end

    describe '#start_consumer_group' do
      it 'starts group and calls on_start with class' do
        router.stop_consumer_group(RouterTestDecider)
        expect(store.consumer_group_active?(RouterTestDecider.group_id)).to be false

        router.start_consumer_group(RouterTestDecider)

        expect(store.consumer_group_active?(RouterTestDecider.group_id)).to be true
      end

      it 'starts group with string group_id' do
        router.stop_consumer_group('router-test-decider')

        router.start_consumer_group('router-test-decider')

        expect(store.consumer_group_active?('router-test-decider')).to be true
      end

      it 'invokes on_start callback on reactor class' do
        allow(RouterTestDecider).to receive(:on_start)

        router.start_consumer_group(RouterTestDecider)

        expect(RouterTestDecider).to have_received(:on_start)
      end
    end

    describe 'resolve_reactor_class' do
      it 'raises ArgumentError for unregistered group_id' do
        expect {
          router.stop_consumer_group('unknown-group')
        }.to raise_error(ArgumentError, /No reactor registered with group_id 'unknown-group'/)
      end
    end

    describe 'no-op default callbacks' do
      it 'works fine when reactor does not override callbacks' do
        # RouterTestProjector has no custom callbacks — should not raise
        expect { router.stop_consumer_group(RouterTestProjector) }.not_to raise_error
        expect { router.reset_consumer_group(RouterTestProjector) }.not_to raise_error
        expect { router.start_consumer_group(RouterTestProjector) }.not_to raise_error
      end
    end
  end

  describe 'simple Consumer reactor (no Decider/Projector)' do
    before do
      router.register(RouterTestAuditReactor)
    end

    it 'registers and processes messages through the router' do
      reg = RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      router.drain

      # Audit message should have been appended
      conds = RouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      audits = store.read(conds).messages
      expect(audits.size).to eq(1)
      expect(audits.first).to be_a(RouterTestMessages::DeviceAudited)
      expect(audits.first.payload.event_type).to eq('router_test.device.registered')
    end

    it 'appended messages are correlated with the source message' do
      reg = RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      router.drain

      conds = RouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      audit = store.read(conds).messages.first

      expect(audit.causation_id).to eq(reg.id)
      expect(audit.correlation_id).to eq(reg.correlation_id)
    end

    it 'handles multiple messages across partitions' do
      store.append(RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor A' }))
      store.append(RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd2', name: 'Sensor B' }))

      router.drain

      d1_conds = RouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      d2_conds = RouterTestMessages::DeviceAudited.to_conditions(device_id: 'd2')

      expect(store.read(d1_conds).messages.size).to eq(1)
      expect(store.read(d2_conds).messages.size).to eq(1)
    end

    it 'context_for returns empty conditions (no evolve types)' do
      expect(RouterTestAuditReactor.context_for(device_id: 'd1')).to eq([])
    end

    it 'works alongside deciders and projectors' do
      router.register(RouterTestDecider)

      reg = RouterTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      store.append(reg)

      cmd = RouterTestMessages::BindDevice.new(payload: { device_id: 'd1', asset_id: 'a1' })
      store.append(cmd)

      router.drain

      # Audit reactor sees DeviceRegistered and DeviceBound
      conds = RouterTestMessages::DeviceAudited.to_conditions(device_id: 'd1')
      audits = store.read(conds).messages
      types = audits.map { |m| m.payload.event_type }.sort

      expect(types).to eq([
        'router_test.device.bound',
        'router_test.device.registered'
      ])
    end
  end
end
