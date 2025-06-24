# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

module ConcurrencyExamples
  SomethingHappened = Sourced::Message.define('concurrent.something_happened') do
    attribute :number, Integer
  end

  class Store
    attr_reader :data, :queue

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

  class Projector < Sourced::Projector::EventSourced
    state do |id|
      STORE.get(id) || {id:, seq: 0, seqs: []}
    end

    sync do |state, _command, _events|
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
    Sequel.sqlite
  end

  let(:router) { Sourced::Router.new(backend:) }

  before do
    if backend.installed?
      # Force drop and recreate tables to get latest schema
      %w[sourced_event_claims sourced_offsets sourced_commands sourced_events sourced_consumer_groups sourced_streams].each do |table|
        db.drop_table?(table.to_sym)
      end
    end
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

    all_events = stream1_events + stream2_events
    all_events.each do |event|
      backend.append_to_stream(event.stream_id, [event])
    end
  end

  specify 'consumes streams concurrently, maintaining per-stream event ordering, consuming all available events for each stream' do
    workers = 2.times.map do |i|
      Sourced::Worker.new(name: "worker-#{i}", router:)
    end

    threads = workers.map do |worker|
      Thread.new do
        worker.poll
      end
    end

    count = 0
    while ConcurrencyExamples::STORE.queue.pop
      count += 1
      if count == 220
        break
      end
    end

    workers.each(&:stop)
    threads.each(&:join)

    expect(ConcurrencyExamples::STORE.data['stream1'][:seq]).to eq(100)
    expect(ConcurrencyExamples::STORE.data['stream1'][:seqs]).to eq((1..100).to_a)

    expect(ConcurrencyExamples::STORE.data['stream2'][:seq]).to eq(120)
    expect(ConcurrencyExamples::STORE.data['stream2'][:seqs]).to eq((1..120).to_a)
  end
end
