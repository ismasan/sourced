# frozen_string_literal: true

module Sourced
  module CCC
    # Periodic loop that heartbeats active workers and releases claims
    # held by workers that have stopped heartbeating (crashed or killed).
    #
    # Combines heartbeating and reaping in one loop since CCC doesn't have
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
      # @param store [CCC::Store] the CCC store
      # @param interval [Numeric] seconds between heartbeat/reap cycles (default 30)
      # @param ttl_seconds [Integer] age threshold for stale claims (default 120)
      # @param worker_ids_provider [Proc] returns Array<String> of active worker IDs
      # @param logger [Object] logger instance
      def initialize(store:, interval: 30, ttl_seconds: 120, worker_ids_provider: -> { [] }, logger: Sourced.config.logger)
        @store = store
        @interval = interval
        @ttl_seconds = ttl_seconds
        @worker_ids_provider = worker_ids_provider
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
        @logger.info 'CCC::StaleClaimReaper: stopped'
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
        @logger.debug "CCC::StaleClaimReaper: heartbeated #{count} workers" if count > 0
      end

      def reap
        released = @store.release_stale_claims(ttl_seconds: @ttl_seconds)
        @logger.info "CCC::StaleClaimReaper: released #{released} stale claims" if released > 0
      end
    end
  end
end
