# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::ErrorStrategy do
  let(:exception) { RuntimeError.new('boom') }
  let(:message)   { double('Message') }
  let(:group) do
    instance_double('Group').tap do |g|
      allow(g).to receive(:error_context).and_return({})
      allow(g).to receive(:retry)
      allow(g).to receive(:fail)
    end
  end

  describe 'defaults' do
    subject(:strategy) { described_class.new }

    it 'has zero retries and a 3-second retry_after' do
      expect(strategy.max_retries).to eq(0)
      expect(strategy.retry_after).to eq(3)
    end

    it 'fails the group immediately when no retries are configured' do
      strategy.call(exception, message, group)
      expect(group).to have_received(:fail).with(exception: exception)
      expect(group).not_to have_received(:retry)
    end

    it 'is frozen after construction' do
      expect(strategy).to be_frozen
    end
  end

  describe '#retry configuration' do
    it 'sets max_retries and retry_after' do
      strategy = described_class.new do |s|
        s.retry(times: 4, after: 10)
      end

      expect(strategy.max_retries).to eq(4)
      expect(strategy.retry_after).to eq(10)
    end

    it 'returns self for chaining' do
      described_class.new do |s|
        expect(s.retry(times: 1)).to be(s)
      end
    end
  end

  describe '#on_retry and #on_fail subscribers' do
    it 'accepts a block' do
      called = nil
      strategy = described_class.new do |s|
        s.retry(times: 1)
        s.on_retry { |*args| called = args }
      end
      allow(group).to receive(:error_context).and_return(retry_count: 1)

      Timecop.freeze(Time.utc(2026, 1, 1)) do
        strategy.call(exception, message, group)
        expect(called).to eq([1, exception, message, Time.utc(2026, 1, 1) + 3])
      end
    end

    it 'accepts a callable' do
      captured = nil
      callback = ->(*args) { captured = args }
      strategy = described_class.new do |s|
        s.on_fail(callback)
      end

      strategy.call(exception, message, group)
      expect(captured).to eq([1, exception, message])
    end

    it 'invokes multiple on_retry subscribers in registration order' do
      calls = []
      strategy = described_class.new do |s|
        s.retry(times: 2)
        s.on_retry { |n, *| calls << [:a, n] }
        s.on_retry { |n, *| calls << [:b, n] }
      end
      allow(group).to receive(:error_context).and_return(retry_count: 1)

      strategy.call(exception, message, group)
      expect(calls).to eq([[:a, 1], [:b, 1]])
    end

    it 'invokes multiple on_fail subscribers' do
      calls = []
      strategy = described_class.new do |s|
        s.on_fail { |n, e, m| calls << [:a, n, e, m] }
        s.on_fail { |n, e, m| calls << [:b, n, e, m] }
      end

      strategy.call(exception, message, group)
      expect(calls).to eq([
        [:a, 1, exception, message],
        [:b, 1, exception, message]
      ])
    end

    it 'returns self from on_retry / on_fail for chaining' do
      described_class.new do |s|
        expect(s.on_retry { }).to be(s)
        expect(s.on_fail  { }).to be(s)
      end
    end

    it 'freezes subscriber lists so they cannot be mutated post-init' do
      strategy = described_class.new do |s|
        s.on_retry { }
        s.on_fail  { }
      end

      expect {
        strategy.instance_variable_get(:@on_retry) << ->(*) {}
      }.to raise_error(FrozenError)

      expect {
        strategy.instance_variable_get(:@on_fail) << ->(*) {}
      }.to raise_error(FrozenError)
    end
  end

  describe '#call with retries configured' do
    it 'schedules a retry using the backoff, increments retry_count, and fires on_retry' do
      backoff = ->(retry_after, retry_count) { retry_after * (2**(retry_count - 1)) }
      retry_args = nil

      strategy = described_class.new do |s|
        s.retry(times: 3, after: 5, backoff: backoff)
        s.on_retry { |*args| retry_args = args }
      end

      allow(group).to receive(:error_context).and_return(retry_count: 2)

      Timecop.freeze(Time.utc(2026, 1, 1)) do
        strategy.call(exception, message, group)

        expected_at = Time.utc(2026, 1, 1) + (5 * 2) # retry_after * 2^(2-1)
        expect(retry_args).to eq([2, exception, message, expected_at])
        expect(group).to have_received(:retry).with(expected_at, retry_count: 3)
        expect(group).not_to have_received(:fail)
      end
    end

    it 'fails the group when retry_count exceeds max_retries, forwarding retry_count to on_fail' do
      fail_args = nil
      strategy = described_class.new do |s|
        s.retry(times: 2)
        s.on_fail { |*args| fail_args = args }
      end

      allow(group).to receive(:error_context).and_return(retry_count: 3)

      strategy.call(exception, message, group)

      expect(fail_args).to eq([3, exception, message])
      expect(group).to have_received(:fail).with(exception: exception)
      expect(group).not_to have_received(:retry)
    end

    it 'treats a missing error_context retry_count as 1' do
      retry_args = nil
      strategy = described_class.new do |s|
        s.retry(times: 1)
        s.on_retry { |*args| retry_args = args }
      end
      allow(group).to receive(:error_context).and_return({})

      strategy.call(exception, message, group)
      expect(retry_args.first).to eq(1)
      expect(group).to have_received(:retry).with(anything, retry_count: 2)
    end
  end
end
