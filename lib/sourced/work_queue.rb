# frozen_string_literal: true

module Sourced
  # Thread/fiber-safe queue that workers block on.
  # Caps entries per reactor to prevent queue explosion during notification bursts
  # while allowing multiple workers to concurrently drain the same reactor
  # on different streams via SKIP LOCKED.
  class WorkQueue
    # @param max_per_reactor [Integer] max queued entries per reactor
    # @param queue [Thread::Queue, Async::Queue] underlying queue from executor
    def initialize(max_per_reactor: 2, queue: Sourced.config.executor.new_queue)
      @max_per_reactor = max_per_reactor
      @queue = queue
      @counts = Hash.new(0)
      @mutex = Mutex.new
    end

    # Enqueue a reactor for processing.
    # No-op if reactor is already at cap.
    # @param reactor [Class] reactor class to enqueue
    # @return [Boolean] true if enqueued, false if at cap
    def push(reactor)
      @mutex.synchronize do
        return false if @counts[reactor] >= @max_per_reactor

        @counts[reactor] += 1
      end
      @queue.push(reactor)
      true
    end

    # Blocking wait for the next reactor.
    # @return [Class, nil] reactor class, or nil (shutdown sentinel)
    def pop
      reactor = @queue.pop
      return nil if reactor.nil?

      @mutex.synchronize { @counts[reactor] -= 1 if @counts[reactor] > 0 }
      reactor
    end

    # Push N nil sentinels to unblock all workers for shutdown.
    # @param worker_count [Integer] number of workers to unblock
    def close(worker_count)
      worker_count.times { @queue.push(nil) }
    end
  end
end
