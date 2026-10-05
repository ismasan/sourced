# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/thread_executor'
require 'timeout'

RSpec.describe Sourced::PeriodicTask do
  let(:task_class) do
    Class.new(described_class) do
      attr_reader :ticks, :first_ticks
      attr_accessor :gate

      def initialize(**)
        super
        @ticks = 0
        @first_ticks = 0
        @gate = nil
      end

      def first_tick = @first_ticks += 1

      def tick
        @ticks += 1
        @gate&.pop # blocks a tick, to stop mid-tick
      end
    end
  end

  it 'logs a tick that raises and keeps going' do
    logger = instance_double('Logger', info: nil)
    failing = Class.new(task_class) do
      def tick
        super
        raise 'boom' if ticks == 1
      end
    end.new(interval: 0.01, logger:)
    expect(logger).to receive(:error).with(/RuntimeError: boom/).once

    failing.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until failing.ticks >= 3 }

    expect(failing).to be_running
    failing.stop
  end

  subject(:periodic) { task_class.new(interval: 0.01) }

  it 'ticks right after starting, then every interval, until stopped' do
    periodic.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until periodic.ticks >= 3 }

    expect(periodic.first_ticks).to eq(1)
    expect(periodic).to be_running
    expect(periodic.stop).to be(true)
    expect(periodic).not_to be_running

    ticks = periodic.ticks
    sleep 0.05
    expect(periodic.ticks).to eq(ticks)
  end

  it 'wakes from its sleep to stop, instead of sleeping the interval out' do
    slow = task_class.new(interval: 60)
    slow.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until slow.running? }

    expect(slow.stop).to be(true)
  end

  it 'runs in threads when the context cannot spawn' do
    periodic.start(Thread.current)
    Timeout.timeout(2) { sleep 0.005 until periodic.ticks >= 1 }

    expect(periodic.stop).to be(true)
  end

  it 'starts again after stopping' do
    periodic.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until periodic.ticks >= 1 }
    periodic.stop

    periodic.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until periodic.first_ticks == 2 }
    expect(periodic).to be_running
    periodic.stop
  end

  it 'is a no-op when started while running' do
    periodic.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until periodic.running? }
    periodic.start(Sourced::ThreadExecutor.new)

    expect(periodic.first_ticks).to eq(1)
    periodic.stop
  end

  it 'spawns one loop when started twice before the first gets to run' do
    task = CollectingTask.new
    periodic.start(task)
    periodic.start(task)

    expect(task.spawned.size).to eq(1)
  end

  it 'stops without waiting when the spawned loop never ran' do
    periodic.start(CollectingTask.new)

    expect(periodic).not_to be_running
    expect(periodic.stop(timeout: 5)).to be(true)
  end

  it 'gives up waiting on a tick in progress after the timeout, and #stop! raises' do
    gate = Thread::Queue.new
    blocked = task_class.new(interval: 0.01)
    blocked.gate = gate
    blocked.start(Sourced::ThreadExecutor.new)
    Timeout.timeout(2) { sleep 0.005 until blocked.ticks >= 1 }

    expect(blocked.stop(timeout: 0.05)).to be(false)
    expect { blocked.stop!(timeout: 0.05) }.to raise_error(Sourced::ShutdownTimeoutError, /still running after 0.05s/)

    gate << :go
    expect(blocked.stop(timeout: 2)).to be(true)
  end

  it 'can run in the caller until stopped from elsewhere' do
    thread = Thread.new { periodic.run }
    Timeout.timeout(2) { sleep 0.005 until periodic.ticks >= 1 }

    periodic.stop
    expect(thread.join(2)).to be(thread)
  end
end
