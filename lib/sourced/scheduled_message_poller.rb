# frozen_string_literal: true

require 'sourced/periodic_task'

module Sourced
  # Periodically promotes due scheduled messages into the main log.
  class ScheduledMessagePoller < PeriodicTask
    # @param store [Sourced::Store] the store containing scheduled messages
    # @param interval [Numeric] polling interval in seconds
    # @param logger [Object] logger instance
    def initialize(store:, interval: 5, logger: NULL_LOGGER)
      super(interval:, logger:)
      @store = store
    end

    private

    def tick
      promoted = @store.update_schedule!
      logger.info "Sourced::ScheduledMessagePoller: appended #{promoted} scheduled messages" if promoted > 0
    end
  end
end
