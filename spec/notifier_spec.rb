# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Notifier do
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }

  let(:msg_type_a) { double('MsgA', type: 'orders.created') }
  let(:msg_type_b) { double('MsgB', type: 'orders.shipped') }
  let(:reactor1) { double('Reactor1', handled_messages: [msg_type_a]) }
  let(:reactor2) { double('Reactor2', handled_messages: [msg_type_a, msg_type_b]) }

  subject(:notifier) do
    described_class.new(
      work_queue: work_queue,
      reactors: [reactor1, reactor2]
    )
  end

  describe '#call' do
    it 'pushes matching reactors to work_queue' do
      notifier.call(['orders.created'])

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop
      expect(popped).to contain_exactly(reactor1, reactor2)
    end

    it 'handles multiple types and deduplicates reactors' do
      notifier.call(['orders.created', 'orders.shipped'])

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop
      expect(popped).to contain_exactly(reactor1, reactor2)
    end

    it 'strips whitespace from type strings' do
      notifier.call([' orders.created '])

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop
      expect(popped).to contain_exactly(reactor1, reactor2)
    end

    it 'ignores unknown types' do
      notifier.call(['unknown.type'])

      # Nothing should be in the queue; verify by pushing a sentinel
      work_queue.push(:sentinel)
      expect(work_queue.pop).to eq(:sentinel)
    end
  end
end

RSpec.describe Sourced::InlineNotifier do
  subject(:notifier) { described_class.new }

  describe '#on_append / #notify' do
    it 'invokes callback with deduped types on notify' do
      received = nil
      notifier.on_append(->(types) { received = types })

      notifier.notify(['a', 'b', 'a'])
      expect(received).to eq(['a', 'b'])
    end

    it 'does not raise when no callback registered' do
      expect { notifier.notify(['a']) }.not_to raise_error
    end
  end

  describe '#start / #stop' do
    it 'are no-ops' do
      expect(notifier.start).to be_nil
      expect(notifier.stop).to be_nil
    end
  end
end
