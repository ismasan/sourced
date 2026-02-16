# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

module ConcurrencyExamples
  SomethingHappened = Sourced::Message.define('concurrent.something_happened') do
    attribute :number, Integer
  end

  class Store
    attr_reader :data, :queue, :trace

    def initialize
      @mutex = Mutex.new
      @queue = Queue.new
      @data = {}
    end

    def get(stream_id)
      @data[stream_id]
    end

    def set(stream_id, state)
      @mutex.synchronize do
        @data[stream_id] = state
        @queue << 1
      end
    end
  end

  STORE = Store.new

  class Projector < Sourced::Projector::StateStored
    state do |id|
      STORE.get(id) || {id:, seq: 0, seqs: []}
    end

    sync do |state:, events:, replaying:|
      STORE.set(state[:id], state)
    end

    event ConcurrencyExamples::SomethingHappened do |state, event|
      state[:seq] = event.seq
      state[:seqs] << event.seq
    end
  end
end

RSpec.describe 'Processing events concurrently', type: :backend do
  subject(:backend) { Sourced::Backends::SequelBackend.new(db) }

  let(:db) do
    # Sequel.sqlite
    Sequel.postgres('sourced_test')
  end

  let(:router) { Sourced::Router.new(backend:) }

  before do
    backend.clear!
    backend.uninstall
    backend.install

    router.register ConcurrencyExamples::Projector

    stream1_events = 100.times.map do |i|
      seq = i + 1
      ConcurrencyExamples::SomethingHappened.parse(stream_id: 'stream1', seq:, payload: { number: seq })
    end

    stream2_events = 120.times.map do |i|
      seq = i + 1
      ConcurrencyExamples::SomethingHappened.parse(stream_id: 'stream2', seq:, payload: { number: seq })
    end

    all_events = (stream2_events + stream1_events).flatten.compact
    all_events.each do |event|
      backend.append_to_stream(event.stream_id, [event])
    end
  end

  specify 'consumes streams concurrently, maintaining per-stream event ordering, consuming all available events for each stream' do
    work_queue = Sourced::WorkQueue.new(max_per_reactor: 3, queue: Queue.new)
    workers = 3.times.map do |i|
      Sourced::Worker.new(work_queue:, name: "worker-#{i}", router:)
    end

    threads = workers.map do |worker|
      Thread.new do
        worker.run
      end
    end

    # Push the reactor to wake up workers
    3.times { work_queue.push(ConcurrencyExamples::Projector) }

    count = 0
    while count < 220
      ConcurrencyExamples::STORE.queue.pop
      count += 1
    end

    workers.each(&:stop)
    work_queue.close(workers.size)
    threads.each(&:join)

    duplicates = ConcurrencyExamples::STORE.data['stream1'][:seqs].group_by(&:itself).select {|k,v| v.size > 1 }
    expect(ConcurrencyExamples::STORE.data['stream1'][:seq]).to eq(100)
    expect(duplicates).to be_empty
    expect(ConcurrencyExamples::STORE.data['stream1'][:seqs]).to eq((1..100).to_a)
    expect(ConcurrencyExamples::STORE.data['stream2'][:seq]).to eq(120)
    expect(ConcurrencyExamples::STORE.data['stream2'][:seqs]).to eq((1..120).to_a)
  end
end
