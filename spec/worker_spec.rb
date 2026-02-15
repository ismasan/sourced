# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Worker do
  let(:router) { instance_double(Sourced::Router) }
  let(:logger) { instance_double('Logger', warn: nil, info: nil) }
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }
  let(:reactor1) { double('Reactor1') }
  let(:reactor2) { double('Reactor2') }

  before do
    allow(router).to receive(:handle_next_event_for_reactor).and_return(false)
  end

  describe '#tick' do
    subject(:worker) do
      described_class.new(
        work_queue: work_queue,
        logger: logger,
        name: 'test-worker',
        router: router
      )
    end

    it 'delegates to Router#handle_next_event_for_reactor with given reactor and worker name' do
      expect(router).to receive(:handle_next_event_for_reactor)
        .with(reactor1, worker.name, batch_size: 1)
        .and_return(true)

      result = worker.tick(reactor1)
      expect(result).to eq(true)
    end

    it 'returns the result from Router#handle_next_event_for_reactor' do
      allow(router).to receive(:handle_next_event_for_reactor).and_return(false)
      expect(worker.tick(reactor1)).to eq(false)

      allow(router).to receive(:handle_next_event_for_reactor).and_return(true)
      expect(worker.tick(reactor1)).to eq(true)
    end
  end

  describe '#run' do
    subject(:worker) do
      described_class.new(
        work_queue: work_queue,
        logger: logger,
        name: 'test-worker',
        router: router,
        max_drain_rounds: 3
      )
    end

    it 'pops reactors from the work queue and drains them' do
      call_count = 0
      allow(router).to receive(:handle_next_event_for_reactor) do
        call_count += 1
        call_count <= 2 # return true for first 2, false for 3rd
      end

      work_queue.push(reactor1)

      # Run in a thread, then stop after processing
      t = Thread.new { worker.run }
      sleep 0.05 # give worker time to drain
      worker.stop
      work_queue.close(1) # unblock the pop
      t.join(1)

      expect(call_count).to be >= 1
    end

    it 'stops on nil sentinel (shutdown)' do
      work_queue.close(1)
      t = Thread.new { worker.run }
      t.join(1)
      expect(t.alive?).to eq(false)
    end
  end

  describe '#drain' do
    subject(:worker) do
      described_class.new(
        work_queue: work_queue,
        logger: logger,
        name: 'test-worker',
        router: router,
        max_drain_rounds: 3
      )
    end

    before do
      # Mark as running so drain loop operates
      worker.instance_variable_set(:@running, true)
    end

    it 'processes messages until none found' do
      call_count = 0
      allow(router).to receive(:handle_next_event_for_reactor) do
        call_count += 1
        call_count <= 2
      end

      worker.drain(reactor1)

      # Should have called 3 times: true, true, false
      expect(call_count).to eq(3)
    end

    it 'stops at max_drain_rounds and re-enqueues the reactor' do
      allow(router).to receive(:handle_next_event_for_reactor).and_return(true)

      worker.drain(reactor1)

      # Should have called exactly max_drain_rounds times
      expect(router).to have_received(:handle_next_event_for_reactor).exactly(3).times

      # Reactor should be re-enqueued
      expect(work_queue.pop).to eq(reactor1)
    end

    it 'does not re-enqueue when fewer than max_drain_rounds processed' do
      call_count = 0
      allow(router).to receive(:handle_next_event_for_reactor) do
        call_count += 1
        call_count <= 1 # only 1 message found
      end

      worker.drain(reactor1)

      # Work queue should be empty (no re-enqueue)
      # Push and pop to verify nothing was there before
      work_queue.push(reactor2)
      expect(work_queue.pop).to eq(reactor2)
    end
  end
end
