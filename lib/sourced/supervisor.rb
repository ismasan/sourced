# frozen_string_literal: true

require 'async'
require 'console'
require 'sourced/worker'
require 'sourced/house_keeper'

module Sourced
  # The Supervisor manages a pool of background workers that process events and commands.
  # It relies on the configured executor (Async by default) to coordinate multiple workers running concurrently
  # and handles graceful shutdown via signal handling.
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
    # This is a convenience method that creates and starts a supervisor.
    #
    # @param args [Hash] Arguments passed to {#initialize}
    # @return [void] This method blocks until the supervisor is stopped
    # @see #initialize
    def self.start(...)
      new(...).start
    end

    # Initialize a new supervisor instance.
    # Workers are created when {#start} is called, not during initialization.
    #
    # @param logger [Object] Logger instance for supervisor output (defaults to configured logger)
    # @param count [Integer] Number of worker fibers to spawn (defaults to 2)
    # @param executor [Object] Executor instance for running concurrent workers (defaults to configured executor)
    def initialize(
      logger: Sourced.config.logger, 
      count: 2,
      housekeeping_count: 1,
      executor: Sourced.config.executor,
      router: Sourced::Router
    )
      @logger = logger
      @count = count
      @housekeeping_count = housekeeping_count
      @executor = executor
      @router = router
      @workers = []
    end

    # Start the supervisor and all worker fibers.
    # This method blocks until the supervisor receives a shutdown signal.
    # Workers are spawned as concurrent tasks using the configured executor 
    # and will begin polling for events and commands immediately.
    # TODO: consistently inject config, defaulting to Sourced.config values
    #
    # @return [void] Blocks until interrupted by signal
    def start
      logger.info("Starting sync supervisor with #{@count} workers and #{@executor} executor")
      set_signal_handlers

      @housekeepers = @housekeeping_count.times.map do |i|
        HouseKeeper.new(
          logger:,
          backend: router.backend,
          name: "HouseKeeper-#{i}",
          interval: Sourced.config.housekeeping_interval,
          heartbeat_interval: Sourced.config.housekeeping_heartbeat_interval,
          claim_ttl_seconds: Sourced.config.housekeeping_claim_ttl_seconds,
          # Provide live worker IDs for heartbeats
          worker_ids_provider: -> { @workers.map(&:name) }
        )
      end

      @workers = @count.times.map do |i|
        # TODO: worker names using Process.pid, current thread and fiber id
        Worker.new(logger:, router:, name: "worker-#{i}")
      end

      @executor.start do |task|
        @housekeepers.each do |hk|
          task.spawn do
            hk.work
          end
        end

        @workers.each do |wrk|
          task.spawn do
            wrk.poll
          end
        end
      end
    end

    # Stop all workers gracefully.
    # Sends stop signals to all workers and waits for them to finish
    # their current work before shutting down.
    #
    # @return [void]
    def stop
      logger.info("Stopping #{@workers.size} workers and #{@housekeepers.size} house-keepers")
      @workers.each(&:stop)
      @housekeepers.each(&:stop)
      logger.info('All workers stopped')
    end

    # Set up signal handlers for graceful shutdown.
    # Traps INT (Ctrl+C) and TERM signals to call {#stop}.
    #
    # @return [void]
    # @api private
    def set_signal_handlers
      Signal.trap('INT') { stop }
      Signal.trap('TERM') { stop }
    end

    private

    attr_reader :logger, :router
  end
end
