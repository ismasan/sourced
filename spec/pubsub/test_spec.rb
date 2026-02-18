# frozen_string_literal: true

require 'spec_helper'
require 'sourced/pubsub/test'

RSpec.describe Sourced::PubSub::Test, type: :backend do
  subject(:pubsub) { described_class.new }

  def make_event(seq:)
    BackendExamples::Tests::SomethingHappened1.parse(stream_id: 's1', seq: seq, payload: { account_id: seq })
  end

  it 'publishes and subscribes to events' do
    received = []

    Sourced.config.executor.start do |t|
      t.spawn do
        channel1 = pubsub.subscribe('test_channel')
        channel1.start do |event, channel|
          received << event
          channel.stop if received.size == 2
        end
      end

      t.spawn do
        sleep 0.0001
        pubsub.publish('test_channel', make_event(seq: 1))
        pubsub.publish('test_channel', make_event(seq: 2))
      end
    end

    expect(received.map(&:type)).to eq(%w[tests.something_happened1 tests.something_happened1])
    expect(received.map(&:seq)).to eq([1, 2])
  end

  it 'multiple subscribers each get their own copy of messages' do
    received1 = []
    received2 = []

    Sourced.config.executor.start do |t|
      t.spawn do
        ch = pubsub.subscribe('ch')
        ch.start do |event, channel|
          received1 << event
          channel.stop if received1.size == 2
        end
      end

      t.spawn do
        ch = pubsub.subscribe('ch')
        ch.start do |event, channel|
          received2 << event
          channel.stop if received2.size == 2
        end
      end

      t.spawn do
        sleep 0.001
        pubsub.publish('ch', make_event(seq: 1))
        pubsub.publish('ch', make_event(seq: 2))
      end
    end

    expect(received1.map(&:seq)).to eq([1, 2])
    expect(received2.map(&:seq)).to eq([1, 2])
  end

  it 'a slow subscriber does not block a fast subscriber' do
    fast_done = Queue.new
    fast_received = []
    slow_received = []

    Sourced.config.executor.start do |t|
      # Fast subscriber
      t.spawn do
        ch = pubsub.subscribe('ch')
        ch.start do |event, channel|
          fast_received << event
          if fast_received.size == 2
            fast_done << true
            channel.stop
          end
        end
      end

      # Slow subscriber — sleeps on each message
      t.spawn do
        ch = pubsub.subscribe('ch')
        ch.start do |event, channel|
          sleep 0.05
          slow_received << event
          channel.stop if slow_received.size == 2
        end
      end

      t.spawn do
        sleep 0.001
        pubsub.publish('ch', make_event(seq: 1))
        pubsub.publish('ch', make_event(seq: 2))
      end
    end

    # Fast subscriber got all messages
    expect(fast_received.map(&:seq)).to eq([1, 2])
    # Slow subscriber also got all messages
    expect(slow_received.map(&:seq)).to eq([1, 2])
  end

  it 'subscribers on different channels are independent' do
    received_a = []
    received_b = []

    Sourced.config.executor.start do |t|
      t.spawn do
        ch = pubsub.subscribe('channel_a')
        ch.start do |event, channel|
          received_a << event
          channel.stop
        end
      end

      t.spawn do
        ch = pubsub.subscribe('channel_b')
        ch.start do |event, channel|
          received_b << event
          channel.stop
        end
      end

      t.spawn do
        sleep 0.001
        pubsub.publish('channel_a', make_event(seq: 1))
        pubsub.publish('channel_b', make_event(seq: 2))
      end
    end

    expect(received_a.map(&:seq)).to eq([1])
    expect(received_b.map(&:seq)).to eq([2])
  end
end
