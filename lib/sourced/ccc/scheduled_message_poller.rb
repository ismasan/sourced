# frozen_string_literal: true

module Sourced
  module CCC
    # Periodically promotes due scheduled messages into the main CCC log.
    class ScheduledMessagePoller
      # @param store [CCC::Store] the CCC store containing scheduled messages
      # @param interval [Numeric] polling interval in seconds
      # @param logger [Object] logger instance
      def initialize(store:, interval: 5, logger: CCC.config.logger)
        @store = store
        @interval = interval
        @logger = logger
        @running = false
      end

      # Run the polling loop until {#stop} is called.
      #
      # @return [void]
      def run
        @running = true
        while @running
          promoted = @store.update_schedule!
          @logger.info "CCC::ScheduledMessagePoller: appended #{promoted} scheduled messages" if promoted > 0
          sleep @interval
        end
        @logger.info 'CCC::ScheduledMessagePoller: stopped'
      end

      # Signal the poller to stop after the current sleep cycle.
      #
      # @return [void]
      def stop
        @running = false
      end
    end
  end
end
