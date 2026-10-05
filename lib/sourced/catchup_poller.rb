# frozen_string_literal: true

require 'sourced/periodic_task'

module Sourced
  # Safety-net poller that periodically pushes all registered reactors into
  # the {WorkQueue}. This ensures workers eventually process all pending work,
  # even when real-time notifications are missed.
  #
  # Covers several edge cases that the real-time notifier cannot:
  # - Startup catch-up (messages appended before the process started)
  # - Missed notifications (connection drops, process restarts, a stopped dispatcher)
  # - Consumer offset resets
  # - Stores whose notifier is in-process only
  #
  # Cheap to run because {WorkQueue} caps entries per reactor, so redundant
  # pushes are silently dropped.
  #
  # @example
  #   poller = CatchUpPoller.new(
  #     work_queue: dispatcher, # anything with #push(reactor)
  #     reactors: [OrderReactor, ShipReactor],
  #     interval: 10
  #   )
  #   poller.start(task) # pushes all reactors right away, then every 10s
  #   poller.stop
  class CatchUpPoller < PeriodicTask
    # @param work_queue [#push] where to push reactors, ex. a {Dispatcher}
    # @param reactors [Array<Class>] reactor classes to push each interval
    # @param interval [Numeric] seconds between pushes (default 5)
    # @param logger [Object] logger instance
    def initialize(work_queue:, reactors:, interval: 5, logger: NULL_LOGGER)
      super(interval:, logger:)
      @work_queue = work_queue
      @reactors = reactors
    end

    private

    def tick
      @reactors.each { |r| @work_queue.push(r) }
    end
  end
end
