# frozen_string_literal: true

require 'async'
require 'console'
require 'sourced/dispatcher'
require 'sourced/house_keeper'

module Sourced
  # The Supervisor manages background workers that process events and commands.
  # It uses a Dispatcher for signal-driven worker dispatch and HouseKeepers
  # for maintenance tasks (heartbeats, stale claim release, scheduled messages).
  #
  # The supervisor automatically sets up signal handlers for INT and TERM signals
  # to ensure workers shut down cleanly when the process is terminated.
  #
  # @example Start a supervisor with 10 workers
  #   Sourced::Supervisor.start(count: 10)
  #
  # @example Create and start manually
  #   supervisor = Sourced::Supervisor.new(count: 5)
  #   supervisor.start  # This will block until interrupted
  class Supervisor
    # Start a new supervisor instance with the given options.
    #
    # @param args [Hash] Arguments passed to {#initialize}
    # @return [void] This method blocks until the supervisor is stopped
    def self.start(...)
      new(...).start
    end

    # Initialize a new supervisor instance.
    #
    # @param logger [Object] Logger instance for supervisor output
    # @param count [Integer] Number of worker fibers to spawn
    # @param batch_size [Integer] Messages per backend fetch
    # @param max_drain_rounds [Integer] Max drain iterations per reactor pickup
    # @param catchup_interval [Numeric] Seconds between catch-up polls
    # @param executor [Object] Executor instance for running concurrent workers
    def initialize(
      logger: Sourced.config.logger,
      count: Sourced.config.worker_count,
      batch_size: Sourced.config.worker_batch_size,
      max_drain_rounds: Sourced.config.max_drain_rounds,
      catchup_interval: Sourced.config.catchup_interval,
      housekeeping_count: Sourced.config.housekeeping_count,
      housekeeping_interval: Sourced.config.housekeeping_interval,
      housekeeping_heartbeat_interval: Sourced.config.housekeeping_heartbeat_interval,
      housekeeping_claim_ttl_seconds: Sourced.config.housekeeping_claim_ttl_seconds,
      executor: Sourced.config.executor,
      router: Sourced::Router
    )
      @logger = logger
      @count = count
      @batch_size = batch_size
      @max_drain_rounds = max_drain_rounds
      @catchup_interval = catchup_interval
      @housekeeping_count = housekeeping_count
      @housekeeping_interval = housekeeping_interval
      @housekeeping_heartbeat_interval = housekeeping_heartbeat_interval
      @housekeeping_claim_ttl_seconds = housekeeping_claim_ttl_seconds
      @executor = executor
      @router = router
    end

    # Start the supervisor, dispatcher, and housekeepers.
    # This method blocks until the supervisor receives a shutdown signal.
    def start
      logger.info("Starting supervisor with #{@count} workers and #{@executor} executor")
      set_signal_handlers

      @dispatcher = Dispatcher.new(
        router: router,
        worker_count: @count,
        batch_size: @batch_size,
        max_drain_rounds: @max_drain_rounds,
        catchup_interval: @catchup_interval,
        logger: logger
      )

      @housekeepers = @housekeeping_count.times.map do |i|
        HouseKeeper.new(
          logger:,
          backend: router.backend,
          name: "HouseKeeper-#{i}",
          interval: @housekeeping_interval,
          heartbeat_interval: @housekeeping_heartbeat_interval,
          claim_ttl_seconds: @housekeeping_claim_ttl_seconds,
          worker_ids_provider: -> { @dispatcher.workers.map(&:name) }
        )
      end

      @executor.start do |task|
        @housekeepers.each do |hk|
          task.spawn do
            hk.work
          end
        end

        @dispatcher.spawn_into(task)
      end
    end

    # Stop all components gracefully.
    def stop
      logger.info("Stopping dispatcher and #{@housekeepers&.size || 0} house-keepers")
      @dispatcher&.stop
      @housekeepers&.each(&:stop)
      logger.info('All workers stopped')
    end

    # Set up signal handlers for graceful shutdown.
    def set_signal_handlers
      Signal.trap('INT') { stop }
      Signal.trap('TERM') { stop }
    end

    private

    attr_reader :logger, :router
  end
end
