# frozen_string_literal: true

require 'console'
require 'sourced/router'

module Sourced
  # Processes messages from a {WorkQueue} in a signal-driven drain loop.
  #
  # Instead of polling all reactors on a timer, workers block on the queue
  # waiting for signals (from {Notifier} or {CatchUpPoller}), then drain all
  # available work for the signaled reactor before blocking again.
  #
  # The drain loop is bounded by +max_drain_rounds+ to prevent a single
  # busy reactor from monopolizing a worker. When the cap is hit, the reactor
  # is re-enqueued so another worker (or this one) can continue later.
  #
  # Multiple workers can drain the same reactor concurrently — each will
  # claim a different stream via the backend's +SKIP LOCKED+ mechanism.
  #
  # @example Signal-driven mode (production)
  #   queue = Sourced::WorkQueue.new(max_per_reactor: 4)
  #   worker = Sourced::Worker.new(work_queue: queue, name: 'worker-0')
  #   worker.run  # blocks, processing reactors popped from queue
  #
  # @example Single-tick for testing
  #   worker = Sourced::Worker.new(work_queue: queue, name: 'test')
  #   worker.tick(MyReactor)  # => true if a message was processed
  class Worker
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

    # Signal the worker to stop after the current drain completes.
    #
    # @return [void]
    def stop
      @running = false
    end

    # Main run loop. Blocks on the {WorkQueue} waiting for reactor signals.
    # For each reactor popped, calls {#drain} to process available messages,
    # then blocks again. Exits when a +nil+ shutdown sentinel is received.
    #
    # @return [void]
    def run
      @running = true

      while @running
        reactor = @work_queue.pop
        break if reactor.nil? # shutdown sentinel

        drain(reactor)
      end

      @logger.info "Worker #{name}: stopped"
    end

    # Drain available messages for a reactor in a bounded loop.
    #
    # Processes up to +max_drain_rounds+ batches. If all rounds are consumed
    # (suggesting more work is available), re-enqueues the reactor into the
    # {WorkQueue} for continued processing by this or another worker.
    #
    # @param reactor [Class] reactor class to drain messages for
    # @return [void]
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

  end
end
