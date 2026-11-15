# frozen_string_literal: true

require 'sourced/periodic_task'

module Sourced
  # Periodic loop that heartbeats active workers and releases claims
  # held by workers that have stopped heartbeating (crashed or killed).
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

      reaped = @store.release_drained_offsets
      logger.info "Sourced::StaleClaimReaper: reaped #{reaped} drained offsets" if reaped > 0

      pruned = @store.prune_orphan_key_pairs
      logger.info "Sourced::StaleClaimReaper: pruned #{pruned} orphan key_pairs" if pruned && pruned > 0

      @store.optimize! # planner statistics, where tables are new or grew
    end
  end
end
