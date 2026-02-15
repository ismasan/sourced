# frozen_string_literal: true

require 'spec_helper'

# Sequel may not be loaded when running with the in-memory TestBackend.
# Define the exception classes we rescue so the specs work without a DB driver.
unless defined?(Sequel::DatabaseError)
  module Sequel
    class DatabaseError < StandardError; end
    class DatabaseDisconnectError < DatabaseError; end
  end
end

RSpec.describe Sourced::Notifier do
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }
  let(:logger) { instance_double('Logger', info: nil, warn: nil, error: nil) }
  let(:db) { double('DB') }
  let(:backend) { double('Backend', pubsub: true) }

  let(:msg_type_a) { double('MsgA', type: 'orders.created') }
  let(:msg_type_b) { double('MsgB', type: 'orders.shipped') }
  let(:reactor1) { double('Reactor1', handled_messages: [msg_type_a]) }
  let(:reactor2) { double('Reactor2', handled_messages: [msg_type_a, msg_type_b]) }

  before do
    allow(backend).to receive(:send).with(:db).and_return(db)
    allow(db).to receive(:adapter_scheme).and_return(:postgres)
  end

  subject(:notifier) do
    described_class.new(
      work_queue: work_queue,
      reactors: [reactor1, reactor2],
      backend: backend,
      logger: logger
    )
  end

  describe '#active?' do
    it 'returns true for PostgreSQL backends' do
      expect(notifier.active?).to be true
    end

    it 'returns false for non-PG backends' do
      allow(db).to receive(:adapter_scheme).and_return(:sqlite)
      expect(notifier.active?).to be false
    end
  end

  describe '#run' do
    context 'non-PG backend' do
      before do
        allow(db).to receive(:adapter_scheme).and_return(:sqlite)
      end

      it 'returns immediately without listening' do
        expect(db).not_to receive(:listen)
        notifier.run
        expect(logger).not_to have_received(:info).with(/listening/)
      end
    end

    context 'processes notifications' do
      it 'pushes matching reactors to work_queue' do
        allow(db).to receive(:listen) do |_channel, **_opts, &block|
          block.call('sourced_new_messages', 123, 'orders.created')
          # stop after one notification
          notifier.stop
        end

        notifier.run

        # Both reactor1 and reactor2 handle orders.created
        popped = []
        popped << work_queue.pop
        popped << work_queue.pop
        expect(popped).to contain_exactly(reactor1, reactor2)
      end

      it 'handles comma-separated payloads' do
        allow(db).to receive(:listen) do |_channel, **_opts, &block|
          block.call('sourced_new_messages', 123, 'orders.created,orders.shipped')
          notifier.stop
        end

        notifier.run

        # reactor1 handles orders.created, reactor2 handles both — deduplicated
        popped = []
        popped << work_queue.pop
        popped << work_queue.pop
        expect(popped).to contain_exactly(reactor1, reactor2)
      end

      it 'ignores unknown types' do
        allow(db).to receive(:listen) do |_channel, **_opts, &block|
          block.call('sourced_new_messages', 123, 'unknown.type')
          notifier.stop
        end

        notifier.run

        # Nothing should be in the queue; verify by pushing a sentinel
        work_queue.push(:sentinel)
        expect(work_queue.pop).to eq(:sentinel)
      end
    end

    context 'reconnection on disconnect' do
      it 'reconnects on Sequel::DatabaseDisconnectError' do
        call_count = 0
        allow(db).to receive(:listen) do |_channel, **opts, &block|
          call_count += 1
          if call_count == 1
            raise Sequel::DatabaseDisconnectError, 'connection lost'
          else
            opts[:after_listen]&.call
            notifier.stop
          end
        end

        allow(notifier).to receive(:sleep) # skip actual wait

        notifier.run

        expect(call_count).to eq(2)
        expect(logger).to have_received(:warn).with(/connection lost.*attempt 1/)
      end

      it 'resets retry counter after successful reconnect' do
        call_count = 0
        allow(db).to receive(:listen) do |_channel, **opts, &block|
          call_count += 1
          case call_count
          when 1
            raise Sequel::DatabaseDisconnectError, 'first disconnect'
          when 2
            # Successful reconnect — after_listen resets retries
            opts[:after_listen]&.call
            # Then another disconnect
            raise Sequel::DatabaseDisconnectError, 'second disconnect'
          when 3
            opts[:after_listen]&.call
            notifier.stop
          end
        end

        allow(notifier).to receive(:sleep)

        notifier.run

        expect(call_count).to eq(3)
        # Both disconnects should be logged as attempt 1 (counter was reset)
        expect(logger).to have_received(:warn).with(/attempt 1/).twice
      end

      it 're-raises after MAX_RECONNECT_ATTEMPTS' do
        allow(db).to receive(:listen).and_raise(Sequel::DatabaseDisconnectError, 'persistent failure')
        allow(notifier).to receive(:sleep)

        expect { notifier.run }.to raise_error(Sequel::DatabaseDisconnectError)

        expect(logger).to have_received(:error).with(/reconnect failed after #{described_class::MAX_RECONNECT_ATTEMPTS}/)
        expect(logger).to have_received(:warn).exactly(described_class::MAX_RECONNECT_ATTEMPTS).times
      end

      it 're-raises non-disconnect errors immediately' do
        allow(db).to receive(:listen).and_raise(Sequel::DatabaseError, 'syntax error')

        expect { notifier.run }.to raise_error(Sequel::DatabaseError, 'syntax error')
        expect(logger).not_to have_received(:warn)
      end

      it 'does not retry during shutdown' do
        allow(db).to receive(:listen) do
          notifier.stop
          raise Sequel::DatabaseDisconnectError, 'connection lost during shutdown'
        end

        expect { notifier.run }.to raise_error(Sequel::DatabaseDisconnectError)
        expect(logger).not_to have_received(:warn)
      end
    end

    context 'backoff timing' do
      it 'uses linear backoff capped at MAX_RECONNECT_WAIT' do
        waits = []
        allow(notifier).to receive(:sleep) { |w| waits << w }

        call_count = 0
        allow(db).to receive(:listen) do |_channel, **opts, &block|
          call_count += 1
          if call_count <= described_class::MAX_RECONNECT_ATTEMPTS
            raise Sequel::DatabaseDisconnectError, 'fail'
          else
            notifier.stop
          end
        end

        # This will raise because call_count reaches MAX+1 which triggers the > MAX check
        # Actually, let's reconsider: at call_count = MAX_RECONNECT_ATTEMPTS, it raises,
        # retries becomes MAX, which is not > MAX, so it sleeps. Then call_count = MAX+1,
        # we don't raise, we stop. That should work.
        notifier.run

        expected = (1..described_class::MAX_RECONNECT_ATTEMPTS).map do |i|
          [described_class::RECONNECT_INTERVAL * i, described_class::MAX_RECONNECT_WAIT].min
        end
        expect(waits).to eq(expected)
      end
    end
  end
end
