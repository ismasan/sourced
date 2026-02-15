# frozen_string_literal: true

module Sourced
  # Thread/fiber-safe bounded queue that workers block on, waiting for reactor signals.
  #
  # Caps pending entries per reactor (default: worker count) so notification bursts
  # are coalesced without unbounded queue growth. Multiple workers can pop the same
  # reactor concurrently — each will process a different stream via SKIP LOCKED.
  #
  # @example Basic usage
  #   queue = WorkQueue.new(max_per_reactor: 4)
  #   queue.push(MyReactor)  # => true
  #   queue.push(MyReactor)  # => true (second slot)
  #   reactor = queue.pop    # => MyReactor (blocks until available)
  #
  # @example Shutdown
  #   queue.close(worker_count)  # pushes nil sentinels to unblock all workers
  class WorkQueue
    # @param max_per_reactor [Integer] maximum queued entries per reactor class.
    #   Set this to the worker count so each worker can have one pending signal.
    # @param queue [Thread::Queue, Async::Queue] underlying queue implementation
    #   provided by the configured executor
    def initialize(max_per_reactor: 2, queue: Sourced.config.executor.new_queue)
      @max_per_reactor = max_per_reactor
      @queue = queue
      @counts = Hash.new(0)
      @mutex = Mutex.new
    end

    # Enqueue a reactor for processing. No-op if the reactor is already at its
    # per-reactor cap, which provides natural coalescing of notification bursts.
    #
    # @param reactor [Class] reactor class to enqueue
    # @return [Boolean] true if enqueued, false if already at cap
    def push(reactor)
      @mutex.synchronize do
        return false if @counts[reactor] >= @max_per_reactor

        @counts[reactor] += 1
      end
      @queue.push(reactor)
      true
    end

    # Block until a reactor is available, then return it.
    # Returns +nil+ when a shutdown sentinel is received (see {#close}).
    #
    # @return [Class, nil] reactor class, or nil on shutdown
    def pop
      reactor = @queue.pop
      return nil if reactor.nil?

      @mutex.synchronize { @counts[reactor] -= 1 if @counts[reactor] > 0 }
      reactor
    end

    # Push +nil+ sentinels to unblock all workers for graceful shutdown.
    # Each worker's run loop treats +nil+ as a signal to exit.
    #
    # @param worker_count [Integer] number of workers to unblock
    # @return [void]
    def close(worker_count)
      worker_count.times { @queue.push(nil) }
    end
  end
end
