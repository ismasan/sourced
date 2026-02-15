# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

RSpec.describe Sourced::Backends::SequelBackend::PGNotifier do
  # LISTEN holds a connection, NOTIFY must go through a different one.
  before(:all) do
    @listen_db = Sequel.postgres('sourced_test')
    @notify_db = Sequel.postgres('sourced_test')
  end

  after(:all) do
    @listen_db&.disconnect
    @notify_db&.disconnect
  end

  let(:listen_db) { @listen_db }
  let(:notify_db) { @notify_db }

  describe '#start / #notify' do
    it 'delivers notifications to the on_append callback' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.on_append(->(types) { received << types })

      Sourced.config.executor.start do |t|
        t.spawn do
          listener.start
        end

        t.spawn do
          # Give the listener time to enter LISTEN
          sleep 0.01
          sender.notify(['orders.created', 'orders.shipped'])
          # Wait for the notification to be delivered, then stop
          sleep 0.01
          listener.stop
        end
      end

      expect(received.size).to eq(1)
      expect(received.first).to contain_exactly('orders.created', 'orders.shipped')
    end

    it 'receives multiple notifications' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.on_append(->(types) { received << types })

      Sourced.config.executor.start do |t|
        t.spawn do
          listener.start
        end

        t.spawn do
          sleep 0.01
          sender.notify(['orders.created'])
          sleep 0.05
          sender.notify(['orders.shipped'])
          sleep 0.01
          listener.stop
        end
      end

      expect(received.size).to eq(2)
      expect(received[0]).to eq(['orders.created'])
      expect(received[1]).to eq(['orders.shipped'])
    end

    it 'deduplicates types in a single notify call' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.on_append(->(types) { received << types })

      Sourced.config.executor.start do |t|
        t.spawn { listener.start }

        t.spawn do
          sleep 0.01
          sender.notify(['orders.created', 'orders.created', 'orders.shipped'])
          sleep 0.01
          listener.stop
        end
      end

      expect(received.size).to eq(1)
      expect(received.first).to contain_exactly('orders.created', 'orders.shipped')
    end
  end

  describe '#stop' do
    it 'breaks out of the listen loop' do
      notifier = described_class.new(db: listen_db)
      notifier.on_append(->(_) {})

      Sourced.config.executor.start do |t|
        t.spawn { notifier.start }

        t.spawn do
          sleep 0.01
          notifier.stop
        end
      end

      # If we get here, start returned — stop worked
    end
  end
end
