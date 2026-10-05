# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

RSpec.describe Sourced::Worker do
  let(:work_queue) { Sourced::WorkQueue.new(queue: Thread::Queue.new) }
  let(:router) { instance_double(Sourced::Router) }
  subject(:worker) { described_class.new(work_queue:, router:) }

  describe '#wait' do
    it "returns right away for a worker that hasn't run" do
      expect(worker.wait(timeout: 5)).to be(true)
    end

    it 'waits for #run to return' do
      thread = Thread.new { worker.run }
      sleep 0.01 until worker.running?

      expect(worker.wait(timeout: 0.05)).to be(false)

      worker.stop
      work_queue.close(1)
      expect(worker.wait(timeout: 2)).to be(true)
      thread.join
    end
  end

  it 'returns from #run right away when stopped before running' do
    worker.stop
    work_queue.push(Class.new) # would be drained if the worker ran

    expect(router).not_to receive(:handle_next_for)
    worker.run

    expect(worker.running?).to be(false)
    expect(worker.wait(timeout: 0)).to be(true)
  end
end
