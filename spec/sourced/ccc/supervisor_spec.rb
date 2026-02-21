# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

RSpec.describe Sourced::CCC::Supervisor do
  let(:executor) { double('Executor') }
  let(:logger) { instance_double('Logger', info: nil, warn: nil) }
  let(:store_notifier) { Sourced::InlineNotifier.new }
  let(:store) { double('Store', notifier: store_notifier) }
  let(:reactor1) { double('Reactor1', handled_messages: [double(type: 'event1')], group_id: 'Reactor1', partition_keys: [:id]) }
  let(:reactors) { [reactor1] }
  let(:router) { instance_double(Sourced::CCC::Router, store: store, reactors: reactors) }
  let(:task) { double('Task', spawn: nil) }
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }

  before do
    allow(Sourced::WorkQueue).to receive(:new).and_return(work_queue)
    allow(executor).to receive(:start).and_yield(task)
    allow(Signal).to receive(:trap)
  end

  describe '.start' do
    it 'creates a new supervisor instance and starts it' do
      supervisor_instance = instance_double(described_class)
      expect(described_class).to receive(:new).with(
        router: router,
        logger: logger,
        count: 3
      ).and_return(supervisor_instance)
      expect(supervisor_instance).to receive(:start)

      described_class.start(router: router, logger: logger, count: 3)
    end
  end

  describe '#start' do
    subject(:supervisor) do
      described_class.new(
        router: router,
        logger: logger,
        count: 2,
        executor: executor
      )
    end

    it 'sets up INT and TERM signal handlers' do
      expect(Signal).to receive(:trap).with('INT')
      expect(Signal).to receive(:trap).with('TERM')
      supervisor.start
    end

    it 'creates Dispatcher with correct params' do
      expect(Sourced::CCC::Dispatcher).to receive(:new).with(
        router: router,
        worker_count: 2,
        batch_size: 50,
        max_drain_rounds: 10,
        catchup_interval: 5,
        housekeeping_interval: 30,
        claim_ttl_seconds: 120,
        logger: logger
      ).and_call_original

      supervisor.start
    end

    it 'passes custom params through to Dispatcher' do
      custom_supervisor = described_class.new(
        router: router,
        logger: logger,
        count: 4,
        batch_size: 100,
        max_drain_rounds: 20,
        catchup_interval: 10,
        housekeeping_interval: 60,
        claim_ttl_seconds: 300,
        executor: executor
      )

      expect(Sourced::CCC::Dispatcher).to receive(:new).with(
        router: router,
        worker_count: 4,
        batch_size: 100,
        max_drain_rounds: 20,
        catchup_interval: 10,
        housekeeping_interval: 60,
        claim_ttl_seconds: 300,
        logger: logger
      ).and_call_original

      custom_supervisor.start
    end

    it 'spawns via executor (notifier + catchup + reaper + 2 workers = 5 spawns)' do
      expect(executor).to receive(:start).and_yield(task)
      # 1 notifier + 1 catchup_poller + 1 stale_claim_reaper + 2 workers = 5 spawns
      expect(task).to receive(:spawn).exactly(5).times

      supervisor.start
    end
  end

  describe '#stop' do
    subject(:supervisor) do
      described_class.new(
        router: router,
        logger: logger,
        count: 2,
        executor: executor
      )
    end

    before do
      supervisor.start
    end

    it 'logs shutdown information' do
      expect(logger).to receive(:info).with('CCC::Supervisor: stopping dispatcher')
      expect(logger).to receive(:info).with('CCC::Supervisor: all workers stopped')
      # Dispatcher also logs
      allow(logger).to receive(:info)
      supervisor.stop
    end

    it 'stops the dispatcher' do
      dispatcher = supervisor.instance_variable_get(:@dispatcher)
      expect(dispatcher).to receive(:stop)
      # Suppress logs from Supervisor#stop
      allow(logger).to receive(:info)
      supervisor.stop
    end
  end

  describe 'signal handling' do
    subject(:supervisor) do
      described_class.new(
        router: router,
        logger: logger,
        executor: executor
      )
    end

    it 'traps INT and TERM signals to call stop' do
      int_handler = nil
      term_handler = nil

      allow(Signal).to receive(:trap) do |signal, &block|
        int_handler = block if signal == 'INT'
        term_handler = block if signal == 'TERM'
      end

      supervisor.start

      expect(supervisor).to receive(:stop)
      int_handler.call

      expect(supervisor).to receive(:stop)
      term_handler.call
    end
  end
end
