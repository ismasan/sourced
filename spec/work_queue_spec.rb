# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::WorkQueue do
  let(:queue) { described_class.new(max_per_reactor: 2, queue: Queue.new) }
  let(:reactor_a) { double('ReactorA') }
  let(:reactor_b) { double('ReactorB') }

  describe '#push / #pop' do
    it 'enqueues and dequeues a reactor' do
      queue.push(reactor_a)
      expect(queue.pop).to eq(reactor_a)
    end

    it 'returns reactors in FIFO order' do
      queue.push(reactor_a)
      queue.push(reactor_b)
      expect(queue.pop).to eq(reactor_a)
      expect(queue.pop).to eq(reactor_b)
    end
  end

  describe 'cap enforcement' do
    it 'rejects pushes beyond max_per_reactor' do
      expect(queue.push(reactor_a)).to eq(true)
      expect(queue.push(reactor_a)).to eq(true)
      expect(queue.push(reactor_a)).to eq(false) # at cap

      # Different reactor is still allowed
      expect(queue.push(reactor_b)).to eq(true)
    end

    it 'allows pushing again after pop decrements the count' do
      queue.push(reactor_a)
      queue.push(reactor_a)
      expect(queue.push(reactor_a)).to eq(false)

      queue.pop # decrements count
      expect(queue.push(reactor_a)).to eq(true)
    end
  end

  describe '#close' do
    it 'pushes nil sentinels to unblock workers' do
      queue.close(3)
      expect(queue.pop).to be_nil
      expect(queue.pop).to be_nil
      expect(queue.pop).to be_nil
    end
  end

  describe 'concurrent push/pop safety' do
    it 'handles concurrent access without errors' do
      q = described_class.new(max_per_reactor: 100, queue: Queue.new)
      results = []
      mutex = Mutex.new

      producers = 5.times.map do
        Thread.new do
          20.times { q.push(reactor_a) }
        end
      end

      consumers = 5.times.map do
        Thread.new do
          20.times do
            r = q.pop
            mutex.synchronize { results << r }
          end
        end
      end

      producers.each(&:join)
      consumers.each(&:join)

      expect(results.size).to eq(100)
      expect(results).to all(eq(reactor_a))
    end
  end
end
