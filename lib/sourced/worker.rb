# frozen_string_literal: true

require 'console'
require 'sourced/router'

module Sourced
  # A Worker processes messages from a WorkQueue in a signal-driven drain loop.
  # Instead of polling all reactors on a timer, workers block on the queue
  # waiting for signals (from Notifier or CatchUpPoller), then drain all
  # available work for the signaled reactor before blocking again.
  #
  # @example Signal-driven mode (production)
  #   queue = Sourced::WorkQueue.new
  #   worker = Sourced::Worker.new(work_queue: queue, name: 'worker-1')
  #   worker.run  # blocks, processing reactors from queue
  #
  # @example Single-tick for testing
  #   worker = Sourced::Worker.new(work_queue: queue, name: 'test')
  #   worker.tick(some_reactor)
  class Worker
    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_INTERVAL = 1   # seconds, multiplied by attempt number
    MAX_RECONNECT_WAIT = 30  # cap

    # @!attribute [r] name
    #   @return [String] Unique identifier for this worker instance
    attr_reader :name

    # @param work_queue [WorkQueue] queue to receive reactor signals from
    # @param router [Router] router for dispatching events
    # @param name [String] unique name for this worker
    # @param batch_size [Integer] messages per backend fetch
    # @param max_drain_rounds [Integer] max consecutive drain iterations per reactor pickup
    # @param logger [Object] logger instance
    def initialize(
      work_queue:,
      router: Sourced::Router,
      name: SecureRandom.hex(4),
      batch_size: 1,
      max_drain_rounds: 10,
      logger: Sourced.config.logger
    )
      @work_queue = work_queue
      @logger = logger
      @running = false
      @name = [Process.pid, name].join('-')
      @router = router
      @batch_size = batch_size
      @max_drain_rounds = max_drain_rounds
    end

    # Signal the worker to stop.
    # The worker will finish its current drain and then stop.
    def stop
      @running = false
    end

    # Main run loop. Blocks on the work queue waiting for reactor signals.
    # Drains all available work for each signaled reactor before blocking again.
    # Reconnects on transient PG disconnects with linear backoff.
    # Re-raises after MAX_RECONNECT_ATTEMPTS consecutive failures.
    def run
      @running = true
      retries = 0

      while @running
        reactor = @work_queue.pop
        break if reactor.nil? # shutdown sentinel

        begin
          drain(reactor)
          retries = 0
        rescue Sequel::DatabaseDisconnectError => e
          raise unless @running # don't retry during shutdown

          retries += 1
          if retries > MAX_RECONNECT_ATTEMPTS
            @logger.error "Worker #{name}: reconnect failed after #{MAX_RECONNECT_ATTEMPTS} attempts"
            raise
          end

          wait = reconnect_wait(retries)
          @logger.warn "Worker #{name}: connection lost (#{e.class}), retrying in #{wait}s (attempt #{retries}/#{MAX_RECONNECT_ATTEMPTS})"
          sleep wait
        end
      end

      @logger.info "Worker #{name}: stopped"
    end

    # Drain available messages for a reactor, up to max_drain_rounds.
    # If the maximum is reached, re-enqueues the reactor for continued processing.
    # @param reactor [Class] reactor to drain
    def drain(reactor)
      rounds = 0
      while @running && rounds < @max_drain_rounds
        found = @router.handle_next_event_for_reactor(reactor, name, batch_size: @batch_size)
        break unless found

        rounds += 1
      end
      # More work likely — re-enqueue so another worker (or this one) continues
      @work_queue.push(reactor) if @running && rounds >= @max_drain_rounds
    end

    # Process one tick of work for a specific reactor.
    # Convenience method for testing.
    #
    # @param reactor [Class] Specific reactor to process
    # @return [Boolean] true if an event was processed, false otherwise
    def tick(reactor)
      @router.handle_next_event_for_reactor(reactor, name, batch_size: @batch_size)
    end

    private

    attr_reader :logger

    def reconnect_wait(retries)
      [RECONNECT_INTERVAL * retries, MAX_RECONNECT_WAIT].min
    end
  end
end
