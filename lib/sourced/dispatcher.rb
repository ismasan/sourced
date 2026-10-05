# frozen_string_literal: true

require 'sourced/work_queue'
require 'sourced/worker'
require 'sourced/spawner'

module Sourced
  # Orchestrator that wires together the signal-driven dispatch pipeline:
  # {WorkQueue}, {NotificationQueuer}, store notifier, and {Worker}s. The
  # {CatchUpPoller} feeding it and the {StaleClaimReaper} heartbeating for its
  # workers are components of their own, depending on it (see {Config}).
  #
  # Does not own the process lifecycle — the caller provides the task/fiber
  # context via {#start}, and triggers shutdown via {#stop}. It can be started
  # again after stopping, ex. when its process is elected to run the workers
  # again: each run gets fresh workers, pollers and work queue.
  #
  # @example Usage with a task runner
  #   dispatcher = Sourced::Dispatcher.new(router: router, worker_count: 4)
  #   executor.start do |task|
  #     dispatcher.start(task)
  #   end
  #   dispatcher.stop
  #
  # @example With custom queue for testing
  #   queue = WorkQueue.new(max_per_reactor: 2, queue: Queue.new)
  #   dispatcher = Sourced::Dispatcher.new(router: router, work_queue: queue)
  class Dispatcher
    # Subscriber for the store notifier. Routes events to the {WorkQueue}
    # by resolving message types or group IDs to reactor classes.
    #
    # Handles two events:
    # - +'messages_appended'+ — comma-separated type strings;
    #   maps types to interested reactors and pushes them
    # - +'reactor_resumed'+ — a consumer group ID;
    #   looks up the reactor and pushes it directly
    class NotificationQueuer
      MESSAGES_APPENDED = 'messages_appended'
      REACTOR_RESUMED = 'reactor_resumed'

      # @param work_queue [WorkQueue] queue to push signaled reactors onto
      # @param reactors [Array<Class>] reactor classes whose +handled_messages+
      #   define the type-to-reactor mapping
      def initialize(work_queue:, reactors:)
        @work_queue = work_queue
        @type_to_reactors = build_type_lookup(reactors)
        @group_id_to_reactor = build_group_id_lookup(reactors)
      end

      # Dispatch a notifier event to the appropriate handler.
      #
      # @param event_name [String] event name
      # @param value [String] event payload
      # @return [void]
      def call(event_name, value)
        case event_name
        when MESSAGES_APPENDED
          types = value.split(',').map(&:strip)
          reactors = types.flat_map { |t| @type_to_reactors.fetch(t, []) }.uniq
          reactors.each { |r| @work_queue.push(r) }
        when REACTOR_RESUMED
          reactor = @group_id_to_reactor[value]
          @work_queue.push(reactor) if reactor
        end
      end

      private

      # @return [Hash{String => Array<Class>}] mapping from type string to reactor classes
      def build_type_lookup(reactors)
        lookup = Hash.new { |h, k| h[k] = [] }
        reactors.each do |reactor|
          reactor.handled_messages.map(&:type).uniq.each do |type|
            lookup[type] << reactor
          end
        end
        lookup
      end

      # @return [Hash{String => Class}] mapping from group_id to reactor class
      def build_group_id_lookup(reactors)
        reactors.each_with_object({}) do |reactor, lookup|
          lookup[reactor.group_id] = reactor
        end
      end
    end

    # Forwards the notification queuer's pushes to the current run's work queue,
    # and drops them while the dispatcher is stopped: a run's catch-up poll
    # finds whatever was appended before it started.
    class QueueSwitch
      def initialize(target)
        @target = target
      end

      attr_writer :target

      # @param reactor [Class]
      # @return [Boolean] whether it was enqueued
      def push(reactor)
        target = @target
        target ? target.push(reactor) : false
      end
    end

    # The parts of one run, from {#start} to {#stop}: a work queue and the
    # workers popping from it. A worker or work queue can't be used again once
    # stopped, so each run gets its own.
    Run = Data.define(:work_queue, :workers)

    # Raised by {#start} while the previous run's workers are still running,
    # after a {#stop} that timed out
    StillRunningError = Class.new(Sourced::Error)

    # @param router [Sourced::Router] the router providing reactors and store
    # @param worker_count [Integer] number of worker fibers to spawn (default 2)
    # @param batch_size [Integer] max messages per claim (default 50)
    # @param max_drain_rounds [Integer] max drain iterations before re-enqueue (default 10)
    # @param shutdown_timeout [Numeric] seconds {#stop} waits for workers to finish
    #   the batches they're processing (default 30)
    # @param work_queue [WorkQueue, nil] optional pre-built queue for the first
    #   run (useful for testing). Later runs build their own
    # @param logger [Object] logger instance
    def initialize(
      router:,
      worker_count: 2,
      batch_size: 50,
      max_drain_rounds: 10,
      shutdown_timeout: 30,
      work_queue: nil,
      logger: NULL_LOGGER
    )
      @logger = logger
      @router = router
      @worker_count = worker_count
      @batch_size = batch_size
      @max_drain_rounds = max_drain_rounds
      @shutdown_timeout = shutdown_timeout
      @reactors = router.reactors.select { |r| r.handled_messages.any? }.to_a.freeze
      @run = nil
      @queue_switch = nil
      @running = false
      @stopped = false

      return if worker_count.zero?

      @run = build_run(work_queue || new_work_queue)

      # Subscribed once: the switch points at whichever run is current
      @queue_switch = QueueSwitch.new(@run.work_queue)
      @store_notifier = router.store.notifier
      @store_notifier.subscribe(NotificationQueuer.new(work_queue: @queue_switch, reactors: @reactors))
    end

    # The reactors this dispatcher routes to: the router's, except those handling no messages
    # @return [Array<Class>]
    attr_reader :reactors

    # The current run's workers: the ones running, or the ones the next
    # {#start} runs. Empty when +worker_count+ is 0.
    # @return [Array<Sourced::Worker>]
    def workers = @run ? @run.workers : []

    # Enqueue a reactor for the current run's workers, ex. from a {CatchUpPoller}.
    # Dropped while stopped, or with no workers.
    # @return [Boolean] whether it was enqueued
    def push(reactor)
      switch = @queue_switch
      switch ? switch.push(reactor) : false
    end

    # Whether it's started, and not stopped since.
    def running? = @running

    # Spawn the store notifier (e.g. PG LISTEN) and N workers into the caller's
    # task context.
    #
    # Can be called again after {#stop}, to run again with fresh workers.
    # A no-op while running.
    #
    # @param task [Object] an executor task or Async::Task to spawn into, else threads (see {Spawner})
    # @return [self]
    # @raise [StillRunningError] if the previous run's workers haven't finished
    #   (a {#stop} that timed out)
    def start(task)
      return self if @running

      if @run.nil? # no workers: nothing to spawn, but it's started
        @running = true
        return self
      end

      if @stopped
        still_running = @run.workers.select(&:running?)
        if still_running.any?
          raise StillRunningError, "can't start: #{still_running.map(&:name).join(', ')} still running from the last run"
        end

        @run = build_run(new_work_queue)
        @queue_switch.target = @run.work_queue
        @stopped = false
      end

      run = @run
      @running = true

      # Store notifier (start — no-op for InlineNotifier). Spawned, so it checks
      # this run is still current when it gets to run: a stop before that has
      # already stopped the notifier.
      Spawner.into(task) { @store_notifier.start if @running && run.equal?(@run) }

      run.workers.each do |w|
        Spawner.into(task) { w.run }
      end

      self
    end

    # Stop all components, close the work queue, and wait for workers to finish
    # the batches they're processing, for up to +shutdown_timeout+ seconds in all.
    # Workers that haven't started running aren't waited for. Notifications
    # are dropped until the next {#start}.
    #
    # Waiting blocks the caller's thread, or yields to the fiber scheduler when
    # called from a fiber (ex. an Async task), so the workers can finish.
    #
    # Once stopped, a no-op that says whether the workers have finished.
    #
    # @return [Boolean] true if every worker finished, false if the timeout expired first
    def stop
      if @run.nil?
        @running = false
        return true
      end
      return workers.none?(&:running?) if @stopped

      run = @run
      @running = false
      @stopped = true
      @queue_switch.target = nil

      @logger.info "Sourced::Dispatcher: stopping #{run.workers.size} workers"
      @store_notifier.stop
      run.workers.each(&:stop)
      run.work_queue.close(run.workers.size)

      unfinished = wait_for_workers(run.workers)
      if unfinished.any?
        @logger.warn "Sourced::Dispatcher: #{unfinished.map(&:name).join(', ')} still running after #{@shutdown_timeout}s"
        return false
      end

      @logger.info 'Sourced::Dispatcher: all workers stopped'
      true
    end

    # {#stop}, raising if workers are still running after the shutdown timeout.
    # The dispatcher component tears down with this, so a shutdown that leaves
    # workers mid-batch fails loudly instead of disconnecting under them.
    #
    # @return [void]
    # @raise [ShutdownTimeoutError]
    def stop!
      return if stop

      names = workers.select(&:running?).map(&:name).join(', ')
      raise ShutdownTimeoutError, "workers still running after #{@shutdown_timeout}s: #{names}"
    end

    private

    def new_work_queue = WorkQueue.new(max_per_reactor: @worker_count)

    # @param work_queue [WorkQueue]
    # @return [Run]
    def build_run(work_queue)
      workers = @worker_count.times.map do |i|
        Worker.new(
          work_queue:,
          router: @router,
          name: "worker-#{i}",
          batch_size: @batch_size,
          max_drain_rounds: @max_drain_rounds,
          logger: @logger
        )
      end

      Run.new(work_queue:, workers:)
    end

    # @param workers [Array<Worker>]
    # @return [Array<Worker>] workers still running when the timeout expired
    def wait_for_workers(workers)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @shutdown_timeout
      workers.reject do |worker|
        remaining = [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
        worker.wait(timeout: remaining)
      end
    end
  end
end
