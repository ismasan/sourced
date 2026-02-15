# frozen_string_literal: true

require 'sourced/work_queue'
require 'sourced/worker'
require 'sourced/notifier'
require 'sourced/catchup_poller'

module Sourced
  # Orchestrator that wires together the signal-driven dispatch pipeline:
  # {WorkQueue}, {Notifier}, {CatchUpPoller}, backend notifier, and {Worker}s.
  #
  # Does not own the process lifecycle — the caller ({Supervisor} or Falcon service)
  # provides the task/fiber context via {#spawn_into}, and triggers shutdown
  # via {#stop}.
  #
  # @example Usage with Supervisor
  #   dispatcher = Dispatcher.new(router: Sourced::Router, worker_count: 4)
  #   executor.start do |task|
  #     dispatcher.spawn_into(task)
  #   end
  #   # On shutdown:
  #   dispatcher.stop
  #
  # @example With custom queue for testing
  #   queue = WorkQueue.new(max_per_reactor: 2, queue: Queue.new)
  #   dispatcher = Dispatcher.new(router: router, work_queue: queue)
  class Dispatcher
    # @return [Array<Worker>] worker instances managed by this dispatcher
    attr_reader :workers

    # @param router [Router] the router providing async_reactors and backend
    # @param worker_count [Integer] number of worker fibers to spawn (default 2)
    # @param batch_size [Integer] messages per backend fetch (default 1)
    # @param max_drain_rounds [Integer] max drain iterations before a worker
    #   re-enqueues a reactor (default 10)
    # @param catchup_interval [Numeric] seconds between catch-up polls (default 5)
    # @param work_queue [WorkQueue, nil] optional pre-built queue (useful for testing
    #   with a plain +Queue.new+ to avoid Async dependency)
    # @param logger [Object] logger instance
    def initialize(
      router:,
      worker_count: 2,
      batch_size: 1,
      max_drain_rounds: 10,
      catchup_interval: 5,
      work_queue: nil,
      logger: Sourced.config.logger
    )
      @logger = logger
      @router = router

      reactors = router.async_reactors.select { |r| r.handled_messages.any? }.to_a

      @work_queue = work_queue || WorkQueue.new(max_per_reactor: worker_count)

      @workers = worker_count.times.map do |i|
        Worker.new(
          work_queue: @work_queue,
          router: router,
          name: "worker-#{i}",
          batch_size: batch_size,
          max_drain_rounds: max_drain_rounds,
          logger: logger
        )
      end

      @notifier = Notifier.new(work_queue: @work_queue, reactors: reactors)
      @backend_notifier = router.backend.notifier
      @backend_notifier.on_append(@notifier)

      @catchup_poller = CatchUpPoller.new(
        work_queue: @work_queue,
        reactors: reactors,
        interval: catchup_interval,
        logger: logger
      )
    end

    # Spawn all component fibers into the caller's task context.
    # Spawns: backend notifier (LISTEN), catch-up poller, and N workers.
    #
    # Supports both the executor's Task (+#spawn+) and Async::Task (+#async+).
    #
    # @param task [Object] an executor task or Async::Task to spawn fibers into
    # @return [void]
    def spawn_into(task)
      s = task.respond_to?(:spawn) ? :spawn : :async

      # Backend notifier (PG LISTEN fiber — no-op for non-PG)
      task.send(s) { @backend_notifier.start }

      # CatchUp poller
      task.send(s) { @catchup_poller.run }

      # Workers
      @workers.each do |w|
        task.send(s) { w.run }
      end
    end

    # Stop all components and close the work queue.
    # Stops in order: backend notifier, catch-up poller, workers, then
    # pushes shutdown sentinels into the queue to unblock any waiting workers.
    #
    # @return [void]
    def stop
      @logger.info "Dispatcher: stopping #{@workers.size} workers"
      @backend_notifier.stop
      @catchup_poller.stop
      @workers.each(&:stop)
      @work_queue.close(@workers.size)
      @logger.info 'Dispatcher: all components stopped'
    end
  end
end
