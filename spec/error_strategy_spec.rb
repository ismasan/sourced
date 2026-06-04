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

    it 'is mutable after construction' do
      expect(strategy).not_to be_frozen
    end
  end

  describe '#retry configuration' do
    it 'sets max_retries and retry_after' do
      strategy = described_class.new
      strategy.retry(times: 4, after: 10)

      expect(strategy.max_retries).to eq(4)
      expect(strategy.retry_after).to eq(10)
    end

    it 'returns self for chaining' do
      strategy = described_class.new
      expect(strategy.retry(times: 1)).to be(strategy)
    end
  end

  describe '#on_retry and #on_fail subscribers' do
    it 'accepts a block that receives keyword arguments' do
      captured = nil
      strategy = described_class.new
      strategy.retry(times: 1)
      strategy.on_retry { |**kwargs| captured = kwargs }
      allow(group).to receive(:error_context).and_return(retry_count: 1)

      Timecop.freeze(Time.utc(2026, 1, 1)) do
        strategy.call(exception, message, group)
        expect(captured).to eq(
          retry_count: 1,
          exception: exception,
          message: message,
          retry_at: Time.utc(2026, 1, 1) + 3
        )
      end
    end

    it 'accepts a callable' do
      captured = nil
      callback = ->(**kwargs) { captured = kwargs }
      strategy = described_class.new
      strategy.on_fail(callback)

      strategy.call(exception, message, group)
      expect(captured).to eq(retry_count: 1, exception: exception, message: message)
    end

    it 'invokes multiple on_retry subscribers in registration order' do
      calls = []
      strategy = described_class.new
      strategy.retry(times: 2)
      strategy.on_retry { |retry_count:, **| calls << [:a, retry_count] }
      strategy.on_retry { |retry_count:, **| calls << [:b, retry_count] }
      allow(group).to receive(:error_context).and_return(retry_count: 1)

      strategy.call(exception, message, group)
      expect(calls).to eq([[:a, 1], [:b, 1]])
    end

    it 'invokes multiple on_fail subscribers' do
      calls = []
      strategy = described_class.new
      strategy.on_fail { |retry_count:, exception:, message:| calls << [:a, retry_count, exception, message] }
      strategy.on_fail { |retry_count:, exception:, message:| calls << [:b, retry_count, exception, message] }

      strategy.call(exception, message, group)
      expect(calls).to eq([
        [:a, 1, exception, message],
        [:b, 1, exception, message]
      ])
    end

    it 'returns self from on_retry / on_fail for chaining' do
      strategy = described_class.new
      expect(strategy.on_retry { }).to be(strategy)
      expect(strategy.on_fail  { }).to be(strategy)
    end

    it 'accepts new subscribers added after construction' do
      calls = []
      strategy = described_class.new
      strategy.on_fail { calls << :first }
      strategy.on_fail { calls << :second }

      strategy.call(exception, message, group)
      expect(calls).to eq(%i[first second])
    end

    it 'rejects new subscribers once frozen' do
      strategy = described_class.new
      strategy.freeze

      expect { strategy.on_retry { } }.to raise_error(FrozenError)
      expect { strategy.on_fail  { } }.to raise_error(FrozenError)
    end

    it 'adapts an object exposing #report_retry into the on_retry subscriber list' do
      reporter = Class.new do
        attr_reader :calls
        def initialize = @calls = []
        def report_retry(retry_count:, exception:, message:, retry_at:)
          @calls << [retry_count, exception, message, retry_at]
        end
      end.new

      strategy = described_class.new
      strategy.retry(times: 1)
      strategy.on_retry(reporter)
      allow(group).to receive(:error_context).and_return(retry_count: 1)

      Timecop.freeze(Time.utc(2026, 1, 1)) do
        strategy.call(exception, message, group)
        expect(reporter.calls).to eq([[1, exception, message, Time.utc(2026, 1, 1) + 3]])
      end
    end

    it 'adapts an object exposing #report_failure into the on_fail subscriber list' do
      reporter = Class.new do
        attr_reader :calls
        def initialize = @calls = []
        def report_failure(retry_count:, exception:, message:)
          @calls << [retry_count, exception, message]
        end
      end.new

      strategy = described_class.new
      strategy.on_fail(reporter)

      strategy.call(exception, message, group)
      expect(reporter.calls).to eq([[1, exception, message]])
    end

    it 'raises ArgumentError when on_retry receives something without #call or #report_retry' do
      expect {
        described_class.new.on_retry(Object.new)
      }.to raise_error(ArgumentError, /on_retry expects a #call or #report_retry/)
    end

    it 'raises ArgumentError when on_fail receives something without #call or #report_failure' do
      expect {
        described_class.new.on_fail(Object.new)
      }.to raise_error(ArgumentError, /on_fail expects a #call or #report_failure/)
    end
  end

  describe '#call with retries configured' do
    it 'schedules a retry using the backoff, increments retry_count, and fires on_retry' do
      backoff = ->(retry_after, retry_count) { retry_after * (2**(retry_count - 1)) }
      captured = nil

      strategy = described_class.new
      strategy.retry(times: 3, after: 5, backoff: backoff)
      strategy.on_retry { |**kwargs| captured = kwargs }

      allow(group).to receive(:error_context).and_return(retry_count: 2)

      Timecop.freeze(Time.utc(2026, 1, 1)) do
        strategy.call(exception, message, group)

        expected_at = Time.utc(2026, 1, 1) + (5 * 2) # retry_after * 2^(2-1)
        expect(captured).to eq(
          retry_count: 2,
          exception: exception,
          message: message,
          retry_at: expected_at
        )
        expect(group).to have_received(:retry).with(expected_at, retry_count: 3)
        expect(group).not_to have_received(:fail)
      end
    end

    it 'fails the group when retry_count exceeds max_retries, forwarding retry_count to on_fail' do
      captured = nil
      strategy = described_class.new
      strategy.retry(times: 2)
      strategy.on_fail { |**kwargs| captured = kwargs }

      allow(group).to receive(:error_context).and_return(retry_count: 3)

      strategy.call(exception, message, group)

      expect(captured).to eq(retry_count: 3, exception: exception, message: message)
      expect(group).to have_received(:fail).with(exception: exception)
      expect(group).not_to have_received(:retry)
    end

    it 'treats a missing error_context retry_count as 1' do
      captured = nil
      strategy = described_class.new
      strategy.retry(times: 1)
      strategy.on_retry { |**kwargs| captured = kwargs }
      allow(group).to receive(:error_context).and_return({})

      strategy.call(exception, message, group)
      expect(captured[:retry_count]).to eq(1)
      expect(group).to have_received(:retry).with(anything, retry_count: 2)
    end
  end
end
