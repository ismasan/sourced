# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

module CCCDispatcherTestMessages
  DeviceRegistered = Sourced::CCC::Message.define('dispatch_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  DeviceBound = Sourced::CCC::Message.define('dispatch_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  BindDevice = Sourced::CCC::Message.define('dispatch_test.bind_device') do
    attribute :device_id, String
    attribute :asset_id, String
  end
end

class DispatchTestDecider < Sourced::CCC::Decider
  partition_by :device_id
  consumer_group 'dispatch-test-decider'

  state { |_| { exists: false, bound: false } }

  evolve CCCDispatcherTestMessages::DeviceRegistered do |state, _evt|
    state[:exists] = true
  end

  evolve CCCDispatcherTestMessages::DeviceBound do |state, _evt|
    state[:bound] = true
  end

  command CCCDispatcherTestMessages::BindDevice do |state, cmd|
    raise 'Not found' unless state[:exists]
    raise 'Already bound' if state[:bound]
    event CCCDispatcherTestMessages::DeviceBound, device_id: cmd.payload.device_id, asset_id: cmd.payload.asset_id
  end
end

class DispatchTestProjector < Sourced::CCC::Projector
  partition_by :device_id
  consumer_group 'dispatch-test-projector'

  state { |_| { devices: [] } }

  evolve CCCDispatcherTestMessages::DeviceRegistered do |state, evt|
    state[:devices] << evt.payload.name
  end

  evolve CCCDispatcherTestMessages::DeviceBound do |state, _evt|
    # noop
  end

  sync do |state:, messages:, replaying:|
    state[:synced] = true
  end
end

RSpec.describe Sourced::CCC::Dispatcher do
  let(:db) { Sequel.sqlite }
  let(:notifier) { Sourced::InlineNotifier.new }
  let(:store) { Sourced::CCC::Store.new(db, notifier: notifier) }
  let(:router) { Sourced::CCC::Router.new(store: store) }
  let(:logger) { instance_double('Logger', info: nil, warn: nil, debug: nil) }
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }

  before do
    store.install!
    router.register(DispatchTestDecider)
    router.register(DispatchTestProjector)
  end

  describe 'Store notifications' do
    it 'append triggers notify_new_messages' do
      expect(notifier).to receive(:notify_new_messages).with(['dispatch_test.device.registered'])

      store.append(
        CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )
    end

    it 'start_consumer_group triggers notify_reactor_resumed' do
      store.stop_consumer_group('dispatch-test-decider')

      expect(notifier).to receive(:notify_reactor_resumed).with('dispatch-test-decider')

      store.start_consumer_group('dispatch-test-decider')
    end

    it 'empty append does not notify' do
      expect(notifier).not_to receive(:notify_new_messages)

      store.append([])
    end
  end

  describe 'batch_size' do
    it 'claim_next with batch_size limits returned messages' do
      # Append 5 messages for the same partition
      5.times do |i|
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: "Sensor #{i}" })
        )
      end

      claim = store.claim_next(
        'dispatch-test-projector',
        partition_by: ['device_id'],
        handled_types: DispatchTestProjector.handled_messages.map(&:type),
        worker_id: 'w1',
        batch_size: 2
      )

      expect(claim).not_to be_nil
      expect(claim.messages.size).to eq(2)
    end

    it 'claim_next without batch_size returns all messages' do
      5.times do |i|
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: "Sensor #{i}" })
        )
      end

      claim = store.claim_next(
        'dispatch-test-projector',
        partition_by: ['device_id'],
        handled_types: DispatchTestProjector.handled_messages.map(&:type),
        worker_id: 'w1'
      )

      expect(claim).not_to be_nil
      expect(claim.messages.size).to eq(5)
    end
  end

  describe Sourced::CCC::Dispatcher::NotificationQueuer do
    let(:queuer) do
      described_class.new(
        work_queue: work_queue,
        reactors: [DispatchTestDecider, DispatchTestProjector]
      )
    end

    it 'maps message types to interested reactors' do
      # DeviceRegistered is handled by projector (via evolve),
      # BindDevice is handled by decider (via command)
      queuer.call('messages_appended', 'dispatch_test.device.registered,dispatch_test.bind_device')

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop

      expect(popped).to contain_exactly(DispatchTestDecider, DispatchTestProjector)
    end

    it 'maps group_id to reactor for reactor_resumed' do
      queuer.call('reactor_resumed', 'dispatch-test-decider')

      popped = work_queue.pop
      expect(popped).to eq(DispatchTestDecider)
    end

    it 'ignores unknown message types' do
      queuer.call('messages_appended', 'unknown.type')

      # Queue should be empty — push a sentinel to avoid blocking
      work_queue.push(nil)
      expect(work_queue.pop).to be_nil
    end

    it 'ignores unknown group_ids' do
      queuer.call('reactor_resumed', 'unknown-group')

      work_queue.push(nil)
      expect(work_queue.pop).to be_nil
    end
  end

  describe Sourced::CCC::Worker do
    let(:worker) do
      described_class.new(
        work_queue: work_queue,
        router: router,
        name: 'test-worker',
        batch_size: 50,
        max_drain_rounds: 10,
        logger: logger
      )
    end

    describe '#tick' do
      it 'processes one claim for a reactor' do
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
        )

        result = worker.tick(DispatchTestProjector)
        expect(result).to be true
      end

      it 'returns false when no work available' do
        result = worker.tick(DispatchTestDecider)
        expect(result).to be false
      end
    end

    describe '#drain' do
      it 'processes until no more work for reactor' do
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor A' })
        )
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd2', name: 'Sensor B' })
        )

        worker.instance_variable_set(:@running, true)
        worker.drain(DispatchTestProjector)

        # Both partitions should have been processed — no more work
        result = worker.tick(DispatchTestProjector)
        expect(result).to be false
      end

      it 're-enqueues reactor when max_drain_rounds reached' do
        # Create a worker with max_drain_rounds: 1
        bounded_worker = described_class.new(
          work_queue: work_queue,
          router: router,
          name: 'bounded',
          batch_size: 50,
          max_drain_rounds: 1,
          logger: logger
        )

        # Append messages for 2 partitions
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'A' })
        )
        store.append(
          CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd2', name: 'B' })
        )

        bounded_worker.instance_variable_set(:@running, true)
        bounded_worker.drain(DispatchTestProjector)

        # Should have re-enqueued — pop it back
        popped = work_queue.pop
        expect(popped).to eq(DispatchTestProjector)
      end
    end
  end

  describe 'Dispatcher wiring' do
    subject(:dispatcher) do
      described_class.new(
        router: router,
        worker_count: 2,
        batch_size: 50,
        max_drain_rounds: 10,
        catchup_interval: 5,
        work_queue: work_queue,
        logger: logger
      )
    end

    it 'creates the requested number of workers' do
      expect(dispatcher.workers.size).to eq(2)
    end

    it 'creates workers with correct names' do
      names = dispatcher.workers.map(&:name)
      expect(names).to include(match(/worker-0$/))
      expect(names).to include(match(/worker-1$/))
    end

    it 'spawns via #spawn when task responds to spawn' do
      task = double('Task')
      # 1 notifier + 1 catchup_poller + 1 stale_claim_reaper + 2 workers = 5 spawns
      expect(task).to receive(:spawn).exactly(5).times
      dispatcher.spawn_into(task)
    end

    it 'spawns via #async when task does not respond to spawn' do
      task = Object.new
      def task.async; end
      expect(task).to receive(:async).exactly(5).times
      dispatcher.spawn_into(task)
    end

    it '#stop stops all components' do
      dispatcher.stop

      dispatcher.workers.each do |w|
        expect(w.instance_variable_get(:@running)).to eq(false)
      end
    end

    it 'creates zero workers when worker_count is 0' do
      d = described_class.new(
        router: router,
        worker_count: 0,
        logger: logger
      )
      expect(d.workers).to be_empty
    end
  end

  describe 'Integration: append → notify → queue → worker' do
    it 'InlineNotifier fires synchronously through the full pipeline' do
      # Build dispatcher which subscribes NotificationQueuer to the store's notifier
      dispatcher = described_class.new(
        router: router,
        worker_count: 1,
        batch_size: 50,
        max_drain_rounds: 10,
        catchup_interval: 60, # long interval — we test synchronous path only
        work_queue: work_queue,
        logger: logger
      )

      # Append triggers notifier → NotificationQueuer → WorkQueue
      store.append(
        CCCDispatcherTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )

      # Pop from queue — should have the reactors that handle this type
      popped = work_queue.pop
      expect([DispatchTestDecider, DispatchTestProjector]).to include(popped)

      # Worker processes the message
      worker = dispatcher.workers.first
      result = worker.tick(popped)
      expect(result).to be true

      dispatcher.stop
    end
  end
end
