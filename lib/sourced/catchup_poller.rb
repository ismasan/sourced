# frozen_string_literal: true

module Sourced
  # Safety-net poller that periodically pushes all reactors into the WorkQueue.
  # Covers: startup catch-up, missed notifications, offset resets, PG connection drops.
  # Cheap because WorkQueue caps per-reactor entries.
  class CatchUpPoller
    # @param work_queue [WorkQueue]
    # @param reactors [Array<Class>] reactor classes to push
    # @param interval [Numeric] seconds between pushes (default 5)
    # @param logger [Object]
    def initialize(work_queue:, reactors:, interval: 5, logger: Sourced.config.logger)
      @work_queue = work_queue
      @reactors = reactors
      @interval = interval
      @logger = logger
      @running = false
    end

    # Run the poller loop. Blocks until stopped.
    # Pushes all reactors immediately on startup (catch-up), then every interval.
    def run
      @running = true
      push_all # immediate catch-up on startup
      while @running
        sleep @interval
        push_all if @running
      end
      @logger.info 'CatchUpPoller: stopped'
    end

    def stop
      @running = false
    end

    private

    def push_all
      @reactors.each { |r| @work_queue.push(r) }
    end
  end
end
