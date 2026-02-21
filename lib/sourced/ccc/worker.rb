# frozen_string_literal: true

module Sourced
  module CCC
    # Processes CCC reactors from a {WorkQueue} in a signal-driven drain loop.
    #
    # Mirrors {Sourced::Worker} but calls {CCC::Router#handle_next_for} instead
    # of the Sourced-specific +handle_next_event_for_reactor+.
    #
    # @example Signal-driven mode (production)
    #   queue = Sourced::WorkQueue.new(max_per_reactor: 4)
    #   worker = CCC::Worker.new(work_queue: queue, router: router, name: 'worker-0')
    #   worker.run  # blocks, processing reactors popped from queue
    #
    # @example Single-tick for testing
    #   worker = CCC::Worker.new(work_queue: queue, router: router, name: 'test')
    #   worker.tick(MyReactor)  # => true if messages were processed
    class Worker
      # @return [String] unique identifier for this worker instance
      attr_reader :name

      # @param work_queue [WorkQueue] queue to receive reactor signals from
      # @param router [CCC::Router] CCC router for dispatching messages
      # @param name [String] unique name for this worker
      # @param batch_size [Integer] max messages per claim
      # @param max_drain_rounds [Integer] max consecutive drain iterations per reactor pickup
      # @param logger [Object] logger instance
      def initialize(
        work_queue:,
        router:,
        name: SecureRandom.hex(4),
        batch_size: 50,
        max_drain_rounds: 10,
        logger: CCC.config.logger
      )
        @work_queue = work_queue
        @router = router
        @name = [Process.pid, name].join('-')
        @batch_size = batch_size
        @max_drain_rounds = max_drain_rounds
        @logger = logger
        @running = false
      end

      # Signal the worker to stop after the current drain completes.
      # @return [void]
      def stop
        @running = false
      end

      # Main run loop. Blocks on the {WorkQueue} waiting for reactor signals.
      # @return [void]
      def run
        @running = true

        while @running
          reactor = @work_queue.pop
          break if reactor.nil? # shutdown sentinel

          drain(reactor)
        end

        @logger.info "CCC::Worker #{name}: stopped"
      end

      # Drain available messages for a reactor in a bounded loop.
      #
      # Processes up to +max_drain_rounds+ batches. If all rounds are consumed,
      # re-enqueues the reactor for fair scheduling across all reactors.
      #
      # @param reactor [Class] reactor class to drain messages for
      # @return [void]
      def drain(reactor)
        rounds = 0
        while @running && rounds < @max_drain_rounds
          found = @router.handle_next_for(reactor, worker_id: name, batch_size: @batch_size)
          break unless found

          rounds += 1
        end
        # More work likely — re-enqueue so another worker (or this one) continues
        @work_queue.push(reactor) if @running && rounds >= @max_drain_rounds
      end

      # Process one tick of work for a specific reactor. Convenience for testing.
      #
      # @param reactor [Class] reactor class to process
      # @return [Boolean] true if messages were processed, false otherwise
      def tick(reactor)
        @router.handle_next_for(reactor, worker_id: name, batch_size: @batch_size)
      end
    end
  end
end
