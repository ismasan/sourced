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

  it 'keeps running when handling a reactor raises, logging the error' do
    log = StringIO.new
    worker = described_class.new(work_queue:, router:, logger: Logger.new(log))
    failing = Class.new { def self.to_s = 'FailingReactor' }
    working = Class.new
    handled = Thread::Queue.new
    allow(router).to receive(:handle_next_for).with(failing, anything).and_raise(Sequel::DatabaseError, 'database is locked')
    allow(router).to receive(:handle_next_for).with(working, anything) { handled << true; false }
    allow(router).to receive(:release_worker_claims).and_return(0)

    thread = Thread.new { worker.run }
    work_queue.push(failing)
    work_queue.push(working)

    expect(handled.pop(timeout: 2)).to be(true)
    expect(worker.running?).to be(true)
    expect(log.string).to match(/FailingReactor raised Sequel::DatabaseError: database is locked/)

    worker.stop
    work_queue.close(1)
    thread.join
  end

  it 'releases the claims a failure left it holding before it claims again, retrying until that succeeds' do
    log = StringIO.new
    worker = described_class.new(work_queue:, router:, logger: Logger.new(log))
    reactor = Class.new { def self.to_s = 'Reactor' }
    calls = []
    releases = [-> { raise Sequel::DatabaseError, 'database is locked' }, -> { 2 }]
    allow(router).to receive(:release_worker_claims) { |id| calls << [:release, id]; releases.shift.call }
    outcomes = [-> { raise Sequel::DatabaseError, 'database is locked' }, -> { false }]
    allow(router).to receive(:handle_next_for) { calls << :claim; outcomes.shift.call }

    worker.send(:drain, reactor) # claiming raises: the worker may be holding a claim
    worker.send(:drain, reactor) # releasing raises: not claimed, still holding
    worker.send(:drain, reactor) # releases, then claims

    expect(calls).to eq([:claim, [:release, worker.name], [:release, worker.name], :claim])
    expect(log.string).to match(/released 2 claims it was left holding/)
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
