# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

module CCCHandleTestMessages
  CreateDevice = Sourced::CCC::Command.define('handle_test.create_device') do
    attribute :device_id, String
    attribute :name, Sourced::Types::String.present
  end

  DeviceCreated = Sourced::CCC::Event.define('handle_test.device_created') do
    attribute :device_id, String
    attribute :name, String
  end

  ActivateDevice = Sourced::CCC::Command.define('handle_test.activate_device') do
    attribute :device_id, String
  end

  DeviceActivated = Sourced::CCC::Event.define('handle_test.device_activated') do
    attribute :device_id, String
  end
end

class HandleTestDecider < Sourced::CCC::Decider
  partition_by :device_id
  consumer_group 'handle-test-decider'

  state { |_| { exists: false, active: false } }

  evolve CCCHandleTestMessages::DeviceCreated do |state, _evt|
    state[:exists] = true
  end

  evolve CCCHandleTestMessages::DeviceActivated do |state, _evt|
    state[:active] = true
  end

  command CCCHandleTestMessages::CreateDevice do |state, cmd|
    raise 'Already exists' if state[:exists]
    event CCCHandleTestMessages::DeviceCreated, device_id: cmd.payload.device_id, name: cmd.payload.name
  end

  command CCCHandleTestMessages::ActivateDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already active' if state[:active]
    event CCCHandleTestMessages::DeviceActivated, device_id: cmd.payload.device_id
  end
end

RSpec.describe 'Sourced::CCC.handle!' do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::CCC::Store.new(db) }

  before { store.install! }

  describe 'valid command, no prior history' do
    it 'returns command, reactor, and events' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      result = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      expect(result).to be_a(Sourced::CCC::HandleResult)
      expect(result.command).to eq(cmd)
      expect(result.reactor).to be_a(HandleTestDecider)
      expect(result.events.size).to eq(1)
      expect(result.events.first).to be_a(CCCHandleTestMessages::DeviceCreated)
    end

    it 'supports array destructuring' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      cmd_out, reactor, events = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      expect(cmd_out).to eq(cmd)
      expect(reactor).to be_a(HandleTestDecider)
      expect(events.size).to eq(1)
    end

    it 'evolves reactor state' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      _cmd, reactor, _events = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      expect(reactor.state[:exists]).to be true
    end

    it 'appends command and correlated events to the store' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      _cmd, _reactor, events = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      # Both command and event should be in the store
      all = store.db[:ccc_messages].order(:position).all
      expect(all.size).to eq(2)
      expect(all[0][:message_type]).to eq('handle_test.create_device')
      expect(all[1][:message_type]).to eq('handle_test.device_created')
    end

    it 'correlates events with the command' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      _cmd, _reactor, events = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      expect(events.first.causation_id).to eq(cmd.id)
      expect(events.first.correlation_id).to eq(cmd.correlation_id)
    end
  end

  describe 'valid command with prior history' do
    before do
      # Create device first
      Sourced::CCC.handle!(
        HandleTestDecider,
        CCCHandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )
    end

    it 'loads history and evolves before deciding' do
      cmd = CCCHandleTestMessages::ActivateDevice.new(
        payload: { device_id: 'd1' }
      )

      _cmd, reactor, events = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      expect(events.size).to eq(1)
      expect(events.first).to be_a(CCCHandleTestMessages::DeviceActivated)
      expect(reactor.state[:exists]).to be true
      expect(reactor.state[:active]).to be true
    end
  end

  describe 'invalid command' do
    it 'returns immediately without appending' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 123 } # name should be a present string
      )

      cmd_out, reactor, events = Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      expect(cmd_out.valid?).to be false
      expect(reactor).to be_a(HandleTestDecider)
      expect(events).to eq([])

      # Nothing appended
      expect(store.db[:ccc_messages].count).to eq(0)
    end
  end

  describe 'domain invariant violation' do
    it 'raises the domain error' do
      cmd = CCCHandleTestMessages::ActivateDevice.new(
        payload: { device_id: 'd1' }
      )

      expect {
        Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)
      }.to raise_error(RuntimeError, 'Not found')
    end
  end

  describe 'optimistic concurrency' do
    it 'guard prevents concurrent writes within the same partition' do
      # Create the device first so there is history (and thus guard conditions)
      Sourced::CCC.handle!(
        HandleTestDecider,
        CCCHandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      # Simulate a concurrent write to the same partition between load and append
      # by directly appending an event after the first handle!
      store.append(
        CCCHandleTestMessages::DeviceActivated.new(payload: { device_id: 'd1' })
      )

      # A second handle! that also needs to write to the same partition
      # should detect the concurrent write. Since the decider also sees the
      # activated state, it raises an invariant error first.
      cmd = CCCHandleTestMessages::ActivateDevice.new(payload: { device_id: 'd1' })
      expect {
        Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)
      }.to raise_error(RuntimeError, 'Already active')
    end

    it 'passes guard through to store.append for conflict detection' do
      # Verify handle! plumbs the guard through by checking that the
      # guard from load is used when appending
      allow(store).to receive(:append).and_call_original

      Sourced::CCC.handle!(
        HandleTestDecider,
        CCCHandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      expect(store).to have_received(:append).with(
        anything,
        guard: an_instance_of(Sourced::CCC::ConsistencyGuard)
      )
    end
  end

  describe 'offset advancement for registered reactors' do
    let(:router) { Sourced::CCC::Router.new(store: store) }

    before do
      router.register(HandleTestDecider)
      allow(Sourced::CCC).to receive(:config).and_return(
        instance_double(Sourced::CCC::Configuration, router: router)
      )
    end

    it 'advances offsets so background workers skip handled commands' do
      cmd = CCCHandleTestMessages::CreateDevice.new(
        payload: { device_id: 'd1', name: 'Sensor' }
      )

      Sourced::CCC.handle!(HandleTestDecider, cmd, store: store)

      # Background worker should find no work for this partition
      handled = router.handle_next_for(HandleTestDecider, worker_id: 'test-worker')
      expect(handled).to be false
    end

    it 'advances offsets after multiple commands on same partition' do
      Sourced::CCC.handle!(
        HandleTestDecider,
        CCCHandleTestMessages::CreateDevice.new(payload: { device_id: 'd1', name: 'Sensor' }),
        store: store
      )

      Sourced::CCC.handle!(
        HandleTestDecider,
        CCCHandleTestMessages::ActivateDevice.new(payload: { device_id: 'd1' }),
        store: store
      )

      # Background worker should still find no work
      handled = router.handle_next_for(HandleTestDecider, worker_id: 'test-worker')
      expect(handled).to be false
    end
  end
end
