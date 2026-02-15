# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Supervisor do
  let(:executor) { double('Executor') }
  let(:logger) { instance_double('Logger', info: nil, warn: nil) }
  let(:backend_notifier) { Sourced::InlineNotifier.new }
  let(:backend) { double('Backend', notifier: backend_notifier) }
  let(:reactor1) { double('Reactor1', handled_messages: [double(type: 'event1')]) }
  let(:reactors) { Set.new([reactor1]) }
  let(:router) { instance_double(Sourced::Router, backend: backend, async_reactors: reactors) }
  let(:housekeeper) { instance_double(Sourced::HouseKeeper, work: nil, stop: nil) }
  let(:task) { double('Task', spawn: nil) }
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }

  before do
    allow(Sourced::HouseKeeper).to receive(:new).and_return(housekeeper)
    allow(Sourced::WorkQueue).to receive(:new).and_return(work_queue)
    allow(executor).to receive(:start).and_yield(task)
    allow(Signal).to receive(:trap)
  end

  describe '#start' do
    subject(:supervisor) do
      described_class.new(
        logger:,
        count: 2,
        housekeeping_count: 1,
        executor: executor,
        router: router
      )
    end

    it 'sets up signal handlers' do
      expect(Signal).to receive(:trap).with('INT')
      expect(Signal).to receive(:trap).with('TERM')
      supervisor.start
    end

    it 'creates the correct number of housekeepers with proper configuration' do
      expect(Sourced::HouseKeeper).to receive(:new).with(
        hash_including(
          logger:,
          backend: router.backend,
          name: 'HouseKeeper-0'
        )
      ).and_return(housekeeper)

      supervisor.start
    end

    it 'spawns tasks for housekeepers and dispatcher components via executor' do
      expect(executor).to receive(:start).and_yield(task)
      # 1 housekeeper + 1 notifier + 1 catchup_poller + 2 workers = 5 spawns
      expect(task).to receive(:spawn).exactly(5).times

      supervisor.start
    end

    it 'provides worker_ids_provider proc to housekeepers that returns live worker names' do
      worker_ids_provider = nil
      allow(Sourced::HouseKeeper).to receive(:new) do |args|
        worker_ids_provider = args[:worker_ids_provider]
        housekeeper
      end

      supervisor.start

      names = worker_ids_provider.call
      expect(names.size).to eq(2)
      expect(names).to all(match(/worker-\d$/))
    end
  end

  describe '#stop' do
    subject(:supervisor) do
      described_class.new(
        logger:,
        count: 2,
        housekeeping_count: 1,
        executor:,
        router:
      )
    end

    before do
      supervisor.start
    end

    it 'logs shutdown information' do
      expect(logger).to receive(:info).with(/Stopping dispatcher/)
      expect(logger).to receive(:info).with('All workers stopped')
      # Dispatcher also logs
      allow(logger).to receive(:info)
      supervisor.stop
    end

    it 'calls stop on all housekeepers' do
      expect(housekeeper).to receive(:stop)
      supervisor.stop
    end
  end

  describe '.start' do
    it 'creates a new supervisor instance and starts it' do
      supervisor_instance = instance_double(described_class)
      expect(described_class).to receive(:new).with(
        logger:,
        count: 3
      ).and_return(supervisor_instance)
      expect(supervisor_instance).to receive(:start)

      described_class.start(logger:, count: 3)
    end
  end

  describe 'signal handling' do
    subject(:supervisor) do
      described_class.new(
        logger:,
        executor:,
        router:
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
