# frozen_string_literal: true

require 'sourced/periodic_task'

module Sourced
  # Periodic loop that heartbeats active workers, releases claims held by
  # workers that have stopped heartbeating (crashed or killed), and runs the
  # store's own housekeeping (+maintain+, see Config::StoreInterface), so a
  # store's housekeeping runs where workers run.
  #
  # Combines heartbeating and reaping in one loop since Sourced doesn't have
  # a separate HouseKeeper like the main Sourced module.
  #
  # +worker_ids_provider+ is a proc that returns current worker names —
  # the {Dispatcher} owns the Worker instances.
  #
  # @example
  #   reaper = StaleClaimReaper.new(
  #     store: store,
  #     interval: 30,
  #     ttl_seconds: 120,
  #     worker_ids_provider: -> { dispatcher.workers.map(&:name) },
  #     logger: logger
  #   )
  #   reaper.start(task) # reaps right away, then heartbeats and reaps every 30s
  #   reaper.stop
  class StaleClaimReaper < PeriodicTask
    # @param store [Sourced::Store] the store
    # @param interval [Numeric] seconds between heartbeat/reap cycles (default 30)
    # @param ttl_seconds [Integer] age threshold for stale claims (default 120)
    # @param worker_ids_provider [Proc] returns Array<String> of active worker IDs
    # @param logger [Object] logger instance
    def initialize(store:, interval: 30, ttl_seconds: 120, worker_ids_provider: -> { [] }, logger: NULL_LOGGER)
      super(interval:, logger:)
      @store = store
      @ttl_seconds = ttl_seconds
      @worker_ids_provider = worker_ids_provider
    end

    private

    # Reap on startup, for claims left by previously killed workers
    def first_tick = reap

    def tick
      heartbeat
      reap
    end

    def heartbeat
      ids = Array(@worker_ids_provider.call).uniq
      count = @store.worker_heartbeat(ids)
      logger.debug "Sourced::StaleClaimReaper: heartbeated #{count} workers" if count > 0
    end

    def reap
      released = @store.release_stale_claims(ttl_seconds: @ttl_seconds)
      logger.info "Sourced::StaleClaimReaper: released #{released} stale claims" if released > 0

      @store.maintain
    end
  end
end
