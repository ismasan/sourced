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
  #     s.on_retry do |n, exception, message, later|
  #       LOGGER.info("Retrying #{n} times")
  #     end
  #
  #     s.on_fail do |retry_count, exception, _message|
  #       Sentry.capture_exception(exception)
  #     end
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
      @on_retry << (callable || blk)
      self
    end

    def on_fail(callable = nil, &blk)
      @on_fail << (callable || blk)
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
        @on_retry.each { |fn| fn.call(retry_count, exception, message, retry_at) }
        retry_count += 1
        group.retry(retry_at, retry_count:)
      else
        @on_fail.each { |fn| fn.call(retry_count, exception, message) }
        group.fail(exception:)
      end
    end

    private

    attr_reader :backoff
  end
end
