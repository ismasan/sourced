# frozen_string_literal: true

require 'spec_helper'
require 'sourced/pubsub/test'

RSpec.describe Sourced::PubSub::Test, type: :backend do
  subject(:pubsub) { described_class.new }

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
        e1 = BackendExamples::Tests::SomethingHappened1.parse(stream_id: 's1', seq: 1, payload: { account_id: 1 })
        e2 = BackendExamples::Tests::SomethingHappened1.parse(stream_id: 's1', seq: 2, payload: { account_id: 2 })
        pubsub.publish('test_channel', e1)
        pubsub.publish('test_channel', e2)
      end
    end

    expect(received.map(&:type)).to eq(%w[tests.something_happened1 tests.something_happened1])
    expect(received.map(&:seq)).to eq([1, 2])
  end
end
