# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::ErrorStrategy do
  let(:group_class) do
    Struct.new(:status, :retry_at, :error_context) do
      def retry(later, ctx = {})
        self.retry_at = later
        self.error_context.merge!(ctx)
        self
      end

      def stop(message: nil)
        self.status = :stopped
        self.error_context[:message] = message if message
        self
      end

      def fail(exception: nil)
        self.status = :failed
        if exception
          self.error_context[:exception_class] = exception.class.to_s
          self.error_context[:exception_message] = exception.message
        end
        self
      end
    end
  end
  let(:group) { group_class.new(:active, nil, {}) }
  let(:exception) { StandardError.new }
  let(:message) { Sourced::Message.new }

  before do
    allow(group).to receive(:retry).and_call_original
    allow(group).to receive(:stop).and_call_original
    allow(group).to receive(:fail).and_call_original
  end

  it 'fails the group immediatly by default' do
    strategy = described_class.new
    strategy.call(exception, message, group)
    expect(group).to have_received(:fail).with(exception:)
  end

  it 'can be configured with retries' do
    now = Time.new(2020, 1, 1).utc

    retries = []
    fail_call = nil

    strategy = described_class.new do |s|
      s.retry(times: 3, after: 5, backoff: ->(retry_after, retry_count) { retry_after * retry_count })

      s.on_retry do |n, exception, message, later|
        retries << [n, exception, message, later]
      end

      s.on_fail do |exception, message|
        fail_call = [exception, message]
      end
    end

    Timecop.freeze(now) do
      strategy.call(exception, message, group)
      strategy.call(exception, message, group)
      strategy.call(exception, message, group)

      expect(fail_call).to be(nil)

      strategy.call(exception, message, group)

      expect(retries).to eq([
        [1, exception, message, now + 5],
        [2, exception, message, now + 10],
        [3, exception, message, now + 15]
      ])

      expect(fail_call).to eq([exception, message])

      expect(group.retry_at).to eq(now + 15)
      expect(group.status).to eq(:failed)
      expect(group.error_context[:retry_count]).to eq(4)
    end
  end
end
