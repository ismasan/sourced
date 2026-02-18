# frozen_string_literal: true

require 'spec_helper'
require 'sourced/pubsub/pg'

RSpec.describe Sourced::PubSub::PG, type: :backend do
  before(:all) do
    @db = Sequel.postgres('sourced_test')
    Sequel.extension :fiber_concurrency if Sourced.config.executor.is_a?(Sourced::AsyncExecutor)
  end

  after(:all) do
    @db&.disconnect
  end

  subject(:pubsub) { described_class.new(db: @db, logger: Sourced.config.logger) }

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
