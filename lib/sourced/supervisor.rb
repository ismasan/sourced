# frozen_string_literal: true

require 'sourced/dispatcher'

module Sourced
  # Top-level process entry point for background workers.
  # Creates a {Dispatcher} (which embeds Workers, CatchUpPoller, notifier,
  # and StaleClaimReaper) and spawns it into an executor.
  #
  # @example Start with defaults
  #   Sourced::Supervisor.start(router: my_router)
  #
  # @example Create and start manually
  #   supervisor = Sourced::Supervisor.new(router: my_router, count: 4)
  #   supervisor.start
  class Supervisor
    # Start a new supervisor instance with the given options.
    #
    # @param args [Hash] Arguments passed to {#initialize}
    # @return [void] This method blocks until the supervisor is stopped
    def self.start(...)
      new(...).start
    end

    # @param router [Sourced::Router] the router providing reactors and store
    # @param logger [Object] Logger instance for supervisor output
    # @param count [Integer] Number of worker fibers to spawn
    # @param batch_size [Integer] Messages per backend fetch
    # @param max_drain_rounds [Integer] Max drain iterations per reactor pickup
    # @param catchup_interval [Numeric] Seconds between catch-up polls
    # @param housekeeping_interval [Numeric] Seconds between heartbeat/reap cycles
    # @param claim_ttl_seconds [Integer] Stale claim age threshold in seconds
    # @param executor [Object] Executor instance for running concurrent workers
    def initialize(
      router: Sourced.router,
      logger: Sourced.config.logger,
      count: Sourced.config.worker_count,
      batch_size: Sourced.config.batch_size,
      max_drain_rounds: Sourced.config.max_drain_rounds,
      catchup_interval: Sourced.config.catchup_interval,
      housekeeping_interval: Sourced.config.housekeeping_interval,
      claim_ttl_seconds: Sourced.config.claim_ttl_seconds,
      executor: Sourced.config.executor
    )
      @router = router
      @logger = logger
      @count = count
      @batch_size = batch_size
      @max_drain_rounds = max_drain_rounds
      @catchup_interval = catchup_interval
      @housekeeping_interval = housekeeping_interval
      @claim_ttl_seconds = claim_ttl_seconds
      @executor = executor
    end

    # Start the supervisor and dispatcher.
    # This method blocks until the supervisor receives a shutdown signal.
    def start
      logger.info("Sourced::Supervisor: starting with #{@count} workers and #{@executor} executor")
      set_signal_handlers

      @dispatcher = Dispatcher.new(
        router: @router,
        worker_count: @count,
        batch_size: @batch_size,
        max_drain_rounds: @max_drain_rounds,
        catchup_interval: @catchup_interval,
        housekeeping_interval: @housekeeping_interval,
        claim_ttl_seconds: @claim_ttl_seconds,
        logger: logger
      )

      @executor.start do |task|
        @dispatcher.spawn_into(task)
      end
    end

    # Stop all components gracefully.
    def stop
      logger.info('Sourced::Supervisor: stopping dispatcher')
      @dispatcher&.stop
      logger.info('Sourced::Supervisor: all workers stopped')
    end

    # Set up signal handlers for graceful shutdown.
    def set_signal_handlers
      Signal.trap('INT') { stop }
      Signal.trap('TERM') { stop }
    end

    private

    attr_reader :logger
  end
end
