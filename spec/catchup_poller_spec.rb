# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::CatchUpPoller do
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }
  let(:logger) { instance_double('Logger', info: nil, error: nil) }
  let(:reactor1) { double('Reactor1') }
  let(:reactors) { [reactor1] }

  describe '#run' do
    it 'pushes all reactors on startup and periodically' do
      poller = described_class.new(
        work_queue: work_queue,
        reactors: reactors,
        interval: 0.01,
        logger: logger
      )

      t = Thread.new { poller.run }
      sleep 0.05
      poller.stop
      t.join(1)

      # Should have pushed reactor1 at least once (immediate catch-up)
      popped = work_queue.pop
      expect(popped).to eq(reactor1)
    end
  end

  describe '#stop' do
    it 'stops the poller loop' do
      poller = described_class.new(
        work_queue: work_queue,
        reactors: reactors,
        interval: 0.01,
        logger: logger
      )

      t = Thread.new { poller.run }
      sleep 0.05
      poller.stop
      t.join(1)

      expect(logger).to have_received(:info).with('CatchUpPoller: stopped')
    end
  end
end
