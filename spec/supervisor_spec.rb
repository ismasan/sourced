# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'timeout'
require 'sourced/thread_executor'

RSpec.describe Sourced::Supervisor do
  let(:config) do
    Sourced::Config.build.tap do |c|
      c.config!('logger') { Sourced::NULL_LOGGER }
      c.config!('workers.count') { 0 }
    end
  end

  let(:handlers) { {} }

  before do
    allow(Signal).to receive(:trap) { |signal, &block| handlers[signal] = block }
  end

  after do
    config.teardown! if config.boot_status == :started
  end

  # Runs the supervisor in a thread, waiting until the tree has started
  def run_in_background(supervisor)
    thread = Thread.new { supervisor.start }
    Timeout.timeout(2) { sleep 0.01 until config.boot_status == :started || !thread.alive? }
    thread
  end

  context 'with a ThreadExecutor' do
    before { config.config!('executor') { Sourced::ThreadExecutor.new } }

    it 'boots the tree, and tears it down on #stop' do
      supervisor = described_class.new(config:)
      thread = run_in_background(supervisor)
      expect(config.boot_status).to eq(:started)
      expect(config['store'].installed?).to be(true)

      supervisor.stop
      thread.join(2)

      expect(thread).not_to be_alive
      expect(config.boot_status).to eq(:torn_down)
    end

    %w[INT TERM].each do |signal|
      it "traps #{signal} to tear down" do
        supervisor = described_class.new(config:)
        thread = run_in_background(supervisor)

        handlers.fetch(signal).call
        thread.join(2)

        expect(config.boot_status).to eq(:torn_down)
      end
    end
  end

  it 'spawns the dispatcher and its own shutdown into the executor task' do
    task = CollectingTask.new
    executor = Class.new do
      define_method(:initialize) { |task| @task = task }
      def start = yield(@task)
    end.new(task)
    config.config!('executor') { executor }
    config.config!('workers.count') { 2 }

    described_class.new(config:).start

    # notifier, catch-up poller, scheduled message poller, reaper, 2 workers, and the shutdown task
    expect(task.spawned.size).to eq(7)
    expect(config.boot_status).to eq(:started)
  end

  it "boots the root of a host's tree" do
    host = Sourced::Component.new
    host.mount('sourced', config)
    host.config!('sourced.executor') { Sourced::ThreadExecutor.new }

    supervisor = described_class.new(config:)
    thread = Thread.new { supervisor.start }
    Timeout.timeout(2) { sleep 0.01 until host.boot_status == :started }

    supervisor.stop
    thread.join(2)

    expect(host.boot_status).to eq(:torn_down)
  end
end
