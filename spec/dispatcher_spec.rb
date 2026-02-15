# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Dispatcher do
  let(:reactor1) { double('Reactor1', handled_messages: [double(type: 'event1')]) }
  let(:reactor2) { double('Reactor2', handled_messages: [double(type: 'event2')]) }
  let(:reactors) { Set.new([reactor1, reactor2]) }
  let(:backend) { double('Backend') }
  let(:router) { instance_double(Sourced::Router, async_reactors: reactors, backend: backend) }
  let(:logger) { instance_double('Logger', info: nil, warn: nil, debug: nil) }

  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }

  subject(:dispatcher) do
    described_class.new(
      router: router,
      worker_count: 2,
      batch_size: 1,
      max_drain_rounds: 10,
      catchup_interval: 5,
      work_queue: work_queue,
      logger: logger
    )
  end

  describe '#workers' do
    it 'creates the requested number of workers' do
      expect(dispatcher.workers.size).to eq(2)
    end

    it 'creates workers with correct names' do
      names = dispatcher.workers.map(&:name)
      expect(names).to include(match(/worker-0$/))
      expect(names).to include(match(/worker-1$/))
    end
  end

  describe '#spawn_into' do
    it 'spawns via #spawn when task responds to spawn (executor Task)' do
      task = double('Task')
      # 1 notifier + 1 catchup_poller + 2 workers = 4 spawns
      expect(task).to receive(:spawn).exactly(4).times
      dispatcher.spawn_into(task)
    end

    it 'spawns via #async when task does not respond to spawn (Async::Task)' do
      task = Object.new
      def task.async; end
      # 1 notifier + 1 catchup_poller + 2 workers = 4 spawns
      expect(task).to receive(:async).exactly(4).times
      dispatcher.spawn_into(task)
    end
  end

  describe '#stop' do
    it 'stops all components' do
      dispatcher.stop

      # Workers should be stopped (running = false)
      dispatcher.workers.each do |w|
        # Workers are stopped — verify by checking they won't loop in run
        expect(w.instance_variable_get(:@running)).to eq(false)
      end
    end
  end
end
