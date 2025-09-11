# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Supervisor do
  let(:executor) { double('Executor') }
  let(:logger) { instance_double('Logger', info: nil, warn: nil) }
  let(:router) { instance_double(Sourced::Router, backend: double('Backend')) }
  let(:worker1) { instance_double(Sourced::Worker, poll: nil, stop: nil, name: 'worker-0') }
  let(:worker2) { instance_double(Sourced::Worker, poll: nil, stop: nil, name: 'worker-1') }
  let(:housekeeper) { instance_double(Sourced::HouseKeeper, work: nil, stop: nil) }
  let(:task) { double('Task', spawn: nil) }

  before do
    allow(Sourced::Worker).to receive(:new).and_return(worker1, worker2)
    allow(Sourced::HouseKeeper).to receive(:new).and_return(housekeeper)
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

    it 'creates the correct number of workers with proper configuration' do
      expect(Sourced::Worker).to receive(:new).with(
        hash_including(
          logger:,
          router:,
          name: 'worker-0'
        )
      ).and_return(worker1)

      expect(Sourced::Worker).to receive(:new).with(
        hash_including(
          logger:,
          router:,
          name: 'worker-1'
        )
      ).and_return(worker2)

      supervisor.start
    end

    it 'spawns tasks for housekeepers and workers via executor' do
      expect(executor).to receive(:start).and_yield(task)
      expect(task).to receive(:spawn).exactly(3).times # 1 housekeeper + 2 workers
      
      supervisor.start
    end

    it 'calls work on housekeepers and poll on workers in spawned tasks' do
      allow(task).to receive(:spawn).and_yield

      expect(housekeeper).to receive(:work)
      expect(worker1).to receive(:poll)
      expect(worker2).to receive(:poll)

      supervisor.start
    end

    it 'provides worker_ids_provider proc to housekeepers that returns live worker names' do
      worker_ids_provider = nil
      allow(Sourced::HouseKeeper).to receive(:new) do |args|
        worker_ids_provider = args[:worker_ids_provider]
        housekeeper
      end

      supervisor.start

      expect(worker_ids_provider.call).to eq(['worker-0', 'worker-1'])
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
      supervisor.start # Create workers and housekeepers
    end

    it 'logs shutdown information' do
      expect(logger).to receive(:info).with(/Stopping 2 workers and 1 house-keepers/)
      expect(logger).to receive(:info).with('All workers stopped')
      supervisor.stop
    end

    it 'calls stop on all workers' do
      expect(worker1).to receive(:stop)
      expect(worker2).to receive(:stop)
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
