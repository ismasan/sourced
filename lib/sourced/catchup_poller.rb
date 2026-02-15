# frozen_string_literal: true

module Sourced
  # Safety-net poller that periodically pushes all registered reactors into
  # the {WorkQueue}. This ensures workers eventually process all pending work,
  # even when real-time notifications are missed.
  #
  # Covers several edge cases that the real-time {Notifier} cannot:
  # - Startup catch-up (messages appended before the process started)
  # - Missed PG notifications (connection drops, process restarts)
  # - Consumer offset resets
  # - Non-PG backends where there is no LISTEN/NOTIFY
  #
  # Cheap to run because {WorkQueue} caps entries per reactor, so redundant
  # pushes are silently dropped.
  #
  # @example
  #   poller = CatchUpPoller.new(
  #     work_queue: queue,
  #     reactors: [OrderReactor, ShipReactor],
  #     interval: 10
  #   )
  #   # In a fiber/thread:
  #   poller.run   # blocks, pushing all reactors every 10s
  #   # From another fiber/thread:
  #   poller.stop  # breaks the loop
  class CatchUpPoller
    # @param work_queue [WorkQueue] queue to push reactors onto
    # @param reactors [Array<Class>] reactor classes to push each interval
    # @param interval [Numeric] seconds between pushes (default 5)
    # @param logger [Object] logger instance
    def initialize(work_queue:, reactors:, interval: 5, logger: Sourced.config.logger)
      @work_queue = work_queue
      @reactors = reactors
      @interval = interval
      @logger = logger
      @running = false
    end

    # Run the poller loop. Blocks until {#stop} is called.
    # Pushes all reactors immediately on startup (catch-up), then every +interval+ seconds.
    #
    # @return [void]
    def run
      @running = true
      push_all # immediate catch-up on startup
      while @running
        sleep @interval
        push_all if @running
      end
      @logger.info 'CatchUpPoller: stopped'
    end

    # Signal the poller to stop after the current sleep cycle.
    #
    # @return [void]
    def stop
      @running = false
    end

    private

    # @return [void]
    def push_all
      @reactors.each { |r| @work_queue.push(r) }
    end
  end
end
