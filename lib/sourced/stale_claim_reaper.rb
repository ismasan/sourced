# frozen_string_literal: true

module Sourced
  # Periodic loop that heartbeats active workers and releases claims
  # held by workers that have stopped heartbeating (crashed or killed).
  #
  # Combines heartbeating and reaping in one loop since Sourced doesn't have
  # a separate HouseKeeper like the main Sourced module.
  #
  # +worker_ids_provider+ is a proc that returns current worker names —
  # injected by the {Dispatcher} which owns the Worker instances.
  #
  # @example
  #   reaper = StaleClaimReaper.new(
  #     store: store,
  #     interval: 30,
  #     ttl_seconds: 120,
  #     worker_ids_provider: -> { workers.map(&:name) },
  #     logger: logger
  #   )
  #   # In a fiber/thread:
  #   reaper.run   # blocks, heartbeating + reaping every 30s
  #   # From another fiber/thread:
  #   reaper.stop  # breaks the loop
  class StaleClaimReaper
    # @param store [Sourced::Store] the store
    # @param interval [Numeric] seconds between heartbeat/reap cycles (default 30)
    # @param ttl_seconds [Integer] age threshold for stale claims (default 120)
    # @param worker_ids_provider [Proc] returns Array<String> of active worker IDs
    # @param optimize_interval [Numeric] seconds between {Store#optimize!} runs
    #   keeping SQLite planner statistics fresh as the log grows (default 3600)
    # @param logger [Object] logger instance
    def initialize(store:, interval: 30, ttl_seconds: 120, worker_ids_provider: -> { [] }, optimize_interval: 3600, logger: Sourced.config.logger)
      @store = store
      @interval = interval
      @ttl_seconds = ttl_seconds
      @worker_ids_provider = worker_ids_provider
      @optimize_interval = optimize_interval
      @last_optimized_at = Time.now
      @logger = logger
      @running = false
    end

    # Run the heartbeat/reap loop. Blocks until {#stop} is called.
    # Reaps on startup (from previous runs where workers were killed).
    #
    # @return [void]
    def run
      @running = true
      reap # reap on startup for claims left by previously killed workers
      while @running
        sleep @interval
        heartbeat if @running
        reap if @running
      end
      @logger.info 'Sourced::StaleClaimReaper: stopped'
    end

    # Signal the reaper to stop after the current sleep cycle.
    #
    # @return [void]
    def stop
      @running = false
    end

    private

    def heartbeat
      ids = Array(@worker_ids_provider.call).uniq
      count = @store.worker_heartbeat(ids)
      @logger.debug "Sourced::StaleClaimReaper: heartbeated #{count} workers" if count > 0
    end

    def reap
      released = @store.release_stale_claims(ttl_seconds: @ttl_seconds)
      @logger.info "Sourced::StaleClaimReaper: released #{released} stale claims" if released > 0

      reaped = @store.release_drained_offsets
      @logger.info "Sourced::StaleClaimReaper: reaped #{reaped} drained offsets" if reaped > 0

      pruned = @store.prune_orphan_key_pairs
      @logger.info "Sourced::StaleClaimReaper: pruned #{pruned} orphan key_pairs" if pruned && pruned > 0

      optimize
    end

    # Periodically refresh SQLite planner statistics. Store#optimize! is bounded
    # by analysis_limit, so this is cheap even on large stores; without fresh
    # stats the claim scan's query plan degrades badly (see Store#optimize!).
    def optimize
      return unless Time.now - @last_optimized_at >= @optimize_interval

      @last_optimized_at = Time.now
      @store.optimize!
      @logger.info 'Sourced::StaleClaimReaper: refreshed store statistics (ANALYZE)'
    end
  end
end
