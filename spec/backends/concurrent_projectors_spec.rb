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
    # Sequel.sqlite
    Sequel.postgres('sourced_test')
  end

  let(:router) { Sourced::Router.new(backend:) }

  before do
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

    limit = Thread.new do
      sleep 4
      ConcurrencyExamples::STORE.queue << nil # Signal to stop
    end

    count = 0
    while (ConcurrencyExamples::STORE.queue.pop && count < 220)
      count += 1
    end

    workers.each(&:stop)
    threads.each(&:join)
    limit.join

    expect(ConcurrencyExamples::STORE.data['stream1'][:seq]).to eq(100)
    puts "COUNT #{count}"
    puts "Stream1 seqs: #{ConcurrencyExamples::STORE.data['stream1'][:seqs].inspect}"
    puts "Stream1 seqs length: #{ConcurrencyExamples::STORE.data['stream1'][:seqs].length}"
    puts "Expected length: 100"
    puts "Duplicates: #{ConcurrencyExamples::STORE.data['stream1'][:seqs].group_by(&:itself).select {|k,v| v.size > 1 }}"
    expect(ConcurrencyExamples::STORE.data['stream1'][:seqs]).to eq((1..100).to_a)
    expect(ConcurrencyExamples::STORE.data['stream2'][:seq]).to eq(120)
    expect(ConcurrencyExamples::STORE.data['stream2'][:seqs]).to eq((1..120).to_a)
  end
end
