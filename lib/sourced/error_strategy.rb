# frozen_string_literal: true

module Sourced
  # Built-in configurable error strategy
  # for handling exceptions raised during processing messages (commands or events)
  # By default it stops the consumer group immediately.
  # It can be configured to retry a number of times with a delay between retries.
  # It can also register callbacks to be called on retry and on stop.
  #
  # @example retry with exponential back off and callbacks
  #   strategy = Sourced::ErrorStrategy.new do |s|
  #     s.retry(times: 3, after: 5, backoff: ->(retry_after, retry_count) { retry_after * retry_count })
  #
  #     s.on_retry do |n, exception, message, later|
  #       LOGGER.info("Retrying #{n} times")
  #     end
  #
  #     s.on_stop do |exception, message|
  #       Sentry.capture_exception(exception)
  #     end
  #   end
  class ErrorStrategy
    MAX_RETRIES = 0
    RETRY_AFTER = 3 # seconds
    BACKOFF = ->(retry_after, retry_count) { retry_after * retry_count }
    NOOP_CALLBACK = ->(*_) {}

    attr_reader :max_retries, :retry_after

    def initialize(&setup)
      @max_retries = MAX_RETRIES
      @retry_after = RETRY_AFTER
      @backoff = BACKOFF
      @on_retry = NOOP_CALLBACK
      @on_stop = NOOP_CALLBACK

      yield(self) if block_given?
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
      @on_retry = callable || blk
    end

    def on_stop(callable = nil, &blk)
      @on_stop = callable || blk
    end

    # The Error Strategy interface
    #
    # @param exception [Exception]
    # @param message [Sourced::Message]
    # @param group [#retry, #stop]
    def call(exception, message, group)
      retry_count = group.error_context[:retry_count] || 1
      if retry_count <= max_retries
        now = Time.now.utc
        later = now + (backoff.call(retry_after, retry_count))
        @on_retry.call(retry_count, exception, message, later)
        retry_count += 1
        group.retry(later, retry_count:)
      else
        @on_stop.call(exception, message)
        group.stop(exception:, message:)
      end
    end

    private

    attr_reader :backoff
  end
end
