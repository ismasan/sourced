# frozen_string_literal: true

module Sourced
  class ErrorStrategy
    MAX_RETRIES = 0
    RETRY_AFTER = 3 # seconds
    BACKOFF = ->(retry_after, retry_count) { retry_after * retry_count }
    NOOP_CALLBACK = ->(*_) {}

    def initialize(&setup)
      @max_retries = MAX_RETRIES
      @retry_after = RETRY_AFTER
      @backoff = BACKOFF
      @on_retry = NOOP_CALLBACK
      @on_stop = NOOP_CALLBACK

      yield(self) if block_given?
      freeze
    end

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

    attr_reader :max_retries, :retry_after, :backoff
  end
end
