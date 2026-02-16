# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Dispatcher::NotificationQueuer do
  let(:work_queue) { Sourced::WorkQueue.new(max_per_reactor: 2, queue: Queue.new) }

  let(:msg_type_a) { double('MsgA', type: 'orders.created') }
  let(:msg_type_b) { double('MsgB', type: 'orders.shipped') }
  let(:consumer_info1) { double('ConsumerInfo1', group_id: 'Reactor1') }
  let(:consumer_info2) { double('ConsumerInfo2', group_id: 'Reactor2') }
  let(:reactor1) { double('Reactor1', handled_messages: [msg_type_a], consumer_info: consumer_info1) }
  let(:reactor2) { double('Reactor2', handled_messages: [msg_type_a, msg_type_b], consumer_info: consumer_info2) }

  subject(:queuer) do
    described_class.new(
      work_queue: work_queue,
      reactors: [reactor1, reactor2]
    )
  end

  describe 'messages_appended' do
    it 'pushes matching reactors to work_queue' do
      queuer.call('messages_appended', 'orders.created')

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop
      expect(popped).to contain_exactly(reactor1, reactor2)
    end

    it 'handles multiple types and deduplicates reactors' do
      queuer.call('messages_appended', 'orders.created,orders.shipped')

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop
      expect(popped).to contain_exactly(reactor1, reactor2)
    end

    it 'strips whitespace from type strings' do
      queuer.call('messages_appended', ' orders.created ')

      popped = []
      popped << work_queue.pop
      popped << work_queue.pop
      expect(popped).to contain_exactly(reactor1, reactor2)
    end

    it 'ignores unknown types' do
      queuer.call('messages_appended', 'unknown.type')

      work_queue.push(:sentinel)
      expect(work_queue.pop).to eq(:sentinel)
    end
  end

  describe 'reactor_resumed' do
    it 'pushes reactor matching the group_id' do
      queuer.call('reactor_resumed', 'Reactor1')

      expect(work_queue.pop).to eq(reactor1)
    end

    it 'ignores unknown group_ids' do
      queuer.call('reactor_resumed', 'Unknown')

      work_queue.push(:sentinel)
      expect(work_queue.pop).to eq(:sentinel)
    end
  end

  describe 'unknown events' do
    it 'ignores them' do
      queuer.call('something_else', 'data')

      work_queue.push(:sentinel)
      expect(work_queue.pop).to eq(:sentinel)
    end
  end
end

RSpec.describe Sourced::InlineNotifier do
  subject(:notifier) { described_class.new }

  describe '#subscribe / #publish' do
    it 'delivers events to subscribers' do
      received = []
      notifier.subscribe(->(event, value) { received << [event, value] })

      notifier.publish('test_event', 'test_value')
      expect(received).to eq([['test_event', 'test_value']])
    end

    it 'delivers to multiple subscribers' do
      received1 = []
      received2 = []
      notifier.subscribe(->(event, value) { received1 << [event, value] })
      notifier.subscribe(->(event, value) { received2 << [event, value] })

      notifier.publish('evt', 'val')
      expect(received1).to eq([['evt', 'val']])
      expect(received2).to eq([['evt', 'val']])
    end

    it 'does not raise when no subscribers registered' do
      expect { notifier.publish('evt', 'val') }.not_to raise_error
    end
  end

  describe '#notify_new_messages' do
    it 'publishes messages_appended with deduped comma-separated types' do
      received = []
      notifier.subscribe(->(event, value) { received << [event, value] })

      notifier.notify_new_messages(['a', 'b', 'a'])
      expect(received).to eq([['messages_appended', 'a,b']])
    end
  end

  describe '#notify_reactor_resumed' do
    it 'publishes reactor_resumed with group_id' do
      received = []
      notifier.subscribe(->(event, value) { received << [event, value] })

      notifier.notify_reactor_resumed('MyReactor')
      expect(received).to eq([['reactor_resumed', 'MyReactor']])
    end
  end

  describe '#start / #stop' do
    it 'are no-ops' do
      expect(notifier.start).to be_nil
      expect(notifier.stop).to be_nil
    end
  end
end
