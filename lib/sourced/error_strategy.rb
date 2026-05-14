# frozen_string_literal: true

module Sourced
  # Built-in configurable error strategy
  # for handling exceptions raised during processing messages (commands or events)
  # By default it marks the consumer group as failed immediately.
  # It can be configured to retry a number of times with a delay between retries.
  # It can also register callbacks to be called on retry and on failure.
  #
  # @example retry with exponential back off and callbacks
  #   strategy = Sourced::ErrorStrategy.new do |s|
  #     s.retry(times: 3, after: 5, backoff: ->(retry_after, retry_count) { retry_after * retry_count })
  #
  #     s.on_retry do |retry_count:, exception:, message:, retry_at:|
  #       LOGGER.info("Retrying #{retry_count} times, next at #{retry_at}")
  #     end
  #
  #     s.on_fail do |retry_count:, exception:, message:|
  #       Sentry.capture_exception(exception)
  #     end
  #
  # Subscribers can also be objects that implement #report_retry / #report_failure
  # (with the same keyword signatures) — useful for instrumentation adapters.
  #   end
  class ErrorStrategy
    MAX_RETRIES = 0
    # seconds
    RETRY_AFTER = 3
    BACKOFF = -> (retry_after, retry_count) { retry_after * retry_count }

    attr_reader :max_retries, :retry_after

    def initialize(&setup)
      @max_retries = MAX_RETRIES
      @retry_after = RETRY_AFTER
      @backoff = BACKOFF
      @on_retry = []
      @on_fail = []

      yield(self) if block_given?

      @on_retry.freeze
      @on_fail.freeze
      freeze
    end

    # @option times [Integer] number of retries. Default: 0
    # @option after [Integer] delay in seconds between retries. Default: 3
    # @option backoff [Proc] a callable that takes retry_after and retry_count and returns the delay for the next retry
    # @return [self]
    def retry(times: nil, after: nil, backoff: nil)
      @max_retries = times if times
      @retry_after = after if after
      @backoff = backoff if backoff
      self
    end

    def on_retry(callable = nil, &blk)
      callable ||= blk
      callable = callable.method(:report_retry) if callable.respond_to?(:report_retry)
      unless callable.respond_to?(:call)
        raise ArgumentError, "on_retry expects a #call or #report_retry interface, but got #{callable.inspect}"
      end

      @on_retry << callable
      self
    end

    def on_fail(callable = nil, &blk)
      callable ||= blk
      callable = callable.method(:report_failure) if callable.respond_to?(:report_failure)
      unless callable.respond_to?(:call)
        raise ArgumentError, "on_fail expects a #call or #report_failure interface, but got #{callable.inspect}"
      end

      @on_fail << callable
      self
    end

    # The Error Strategy interface
    #
    # @param exception [Exception]
    # @param message [Sourced::Message]
    # @param group [#retry, #fail]
    def call(exception, message, group)
      retry_count = group.error_context[:retry_count] || 1
      if retry_count <= max_retries
        now = Time.now
        retry_at = now + (backoff.call(retry_after, retry_count))
        safe_dispatch do
          @on_retry.each { |fn| fn.call(retry_count:, exception:, message:, retry_at:) }
        end

        retry_count += 1
        group.retry(retry_at, retry_count:)
      else
        safe_dispatch do
          @on_fail.each { |fn| fn.call(retry_count:, exception:, message:) }
        end

        group.fail(exception:)
      end
    end

    private

    attr_reader :backoff

    def safe_dispatch(&)
      begin
        yield
      rescue StandardError => e
        Console.error(exception: e)
      end
    end
  end
end
