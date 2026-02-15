# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Notifier do
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }
  let(:logger) { instance_double('Logger', info: nil) }
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

  end
end
