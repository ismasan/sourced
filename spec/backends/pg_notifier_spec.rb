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

  describe '#notify_new_messages' do
    it 'delivers type notifications to subscribers' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.subscribe(->(event, value) { received << [event, value] })

      Sourced.config.executor.start do |t|
        t.spawn { listener.start }

        t.spawn do
          sleep 0.01
          sender.notify_new_messages(['orders.created', 'orders.shipped'])
          sleep 0.01
          listener.stop
        end
      end

      expect(received.size).to eq(1)
      expect(received.first).to eq(['messages_appended', 'orders.created,orders.shipped'])
    end

    it 'deduplicates types' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.subscribe(->(event, value) { received << [event, value] })

      Sourced.config.executor.start do |t|
        t.spawn { listener.start }

        t.spawn do
          sleep 0.01
          sender.notify_new_messages(['orders.created', 'orders.created', 'orders.shipped'])
          sleep 0.01
          listener.stop
        end
      end

      expect(received.size).to eq(1)
      expect(received.first).to eq(['messages_appended', 'orders.created,orders.shipped'])
    end
  end

  describe '#notify_reactor_resumed' do
    it 'delivers reactor notifications to subscribers' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.subscribe(->(event, value) { received << [event, value] })

      Sourced.config.executor.start do |t|
        t.spawn { listener.start }

        t.spawn do
          sleep 0.01
          sender.notify_reactor_resumed('OrderReactor')
          sleep 0.01
          listener.stop
        end
      end

      expect(received).to eq([['reactor_resumed', 'OrderReactor']])
    end
  end

  describe 'multiplexing' do
    it 'routes type and reactor notifications correctly over the same channel' do
      listener = described_class.new(db: listen_db)
      sender = described_class.new(db: notify_db)

      received = []
      listener.subscribe(->(event, value) { received << [event, value] })

      Sourced.config.executor.start do |t|
        t.spawn { listener.start }

        t.spawn do
          sleep 0.01
          sender.notify_new_messages(['orders.created'])
          sleep 0.01
          sender.notify_reactor_resumed('ShipReactor')
          sleep 0.01
          listener.stop
        end
      end

      expect(received).to eq([
        ['messages_appended', 'orders.created'],
        ['reactor_resumed', 'ShipReactor']
      ])
    end
  end

  describe '#stop' do
    it 'breaks out of the listen loop' do
      notifier = described_class.new(db: listen_db)

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
