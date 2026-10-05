# frozen_string_literal: true

require 'sourced/work_queue'
require 'sourced/catchup_poller'
require 'sourced/worker'
require 'sourced/scheduled_message_poller'
require 'sourced/stale_claim_reaper'

module Sourced
  # Orchestrator that wires together the signal-driven dispatch pipeline:
  # {WorkQueue}, {NotificationQueuer}, {CatchUpPoller}, store notifier, and {Worker}s.
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
    # Raised by {#stop!} when workers are still running after the shutdown timeout
    ShutdownTimeoutError = Class.new(Sourced::Error)

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

    # The parts of one run, from {#start} to {#stop}: a work queue, the workers
    # popping from it, and the pollers feeding it. A worker or work queue can't
    # be used again once stopped, so each run gets its own.
    Run = Data.define(:work_queue, :workers, :catchup_poller, :scheduled_message_poller, :stale_claim_reaper)

    # Raised by {#start} while the previous run's workers are still running,
    # after a {#stop} that timed out
    StillRunningError = Class.new(Sourced::Error)

    # @param router [Sourced::Router] the router providing reactors and store
    # @param worker_count [Integer] number of worker fibers to spawn (default 2)
    # @param batch_size [Integer] max messages per claim (default 50)
    # @param max_drain_rounds [Integer] max drain iterations before re-enqueue (default 10)
    # @param catchup_interval [Numeric] seconds between catch-up polls (default 5)
    # @param housekeeping_interval [Numeric] seconds between heartbeat/reap cycles (default 30)
    # @param claim_ttl_seconds [Integer] stale claim age threshold in seconds (default 120)
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
      catchup_interval: 5,
      housekeeping_interval: 30,
      claim_ttl_seconds: 120,
      shutdown_timeout: 30,
      work_queue: nil,
      logger: NULL_LOGGER
    )
      @logger = logger
      @router = router
      @worker_count = worker_count
      @batch_size = batch_size
      @max_drain_rounds = max_drain_rounds
      @catchup_interval = catchup_interval
      @housekeeping_interval = housekeeping_interval
      @claim_ttl_seconds = claim_ttl_seconds
      @shutdown_timeout = shutdown_timeout
      @run = nil
      @running = false
      @stopped = false

      return if worker_count.zero?

      @reactors = router.reactors.select { |r| r.handled_messages.any? }.to_a
      @run = build_run(work_queue || new_work_queue)

      # Subscribed once: the switch points at whichever run is current
      @queue_switch = QueueSwitch.new(@run.work_queue)
      @store_notifier = router.store.notifier
      @store_notifier.subscribe(NotificationQueuer.new(work_queue: @queue_switch, reactors: @reactors))
    end

    # The current run's workers: the ones running, or the ones the next
    # {#start} runs. Empty when +worker_count+ is 0.
    # @return [Array<Sourced::Worker>]
    def workers = @run ? @run.workers : []

    # Whether it's started, and not stopped since.
    def running? = @running

    # Spawn all component fibers into the caller's task context.
    # Spawns: store notifier (e.g. PG LISTEN), catch-up poller, scheduled message
    # poller, stale claim reaper, and N workers.
    #
    # Can be called again after {#stop}, to run again with fresh workers.
    # A no-op while running.
    #
    # @param task [Object] an executor task or Async::Task to spawn fibers into
    # @return [self]
    # @raise [ArgumentError] if there are workers to run and +task+ can't spawn them
    # @raise [StillRunningError] if the previous run's workers haven't finished
    #   (a {#stop} that timed out)
    def start(task)
      return self if @run.nil? || @running

      s = %i[spawn async].find { |m| task.respond_to?(m) }
      unless s
        raise ArgumentError, "can't spawn #{@worker_count} workers into #{task.inspect}: " \
                             'start with an Async::Task, or Sourced::ThreadExecutor.new to run them in threads, ' \
                             'or set workers.count to 0 to run no workers in this process'
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

      # Store notifier (start — no-op for InlineNotifier)
      task.send(s) { @store_notifier.start }

      task.send(s) { run.catchup_poller.run }
      task.send(s) { run.scheduled_message_poller.run }
      task.send(s) { run.stale_claim_reaper.run }

      run.workers.each do |w|
        task.send(s) { w.run }
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
      return true if @run.nil?
      return workers.none?(&:running?) if @stopped

      run = @run
      @running = false
      @stopped = true
      @queue_switch.target = nil

      @logger.info "Sourced::Dispatcher: stopping #{run.workers.size} workers"
      @store_notifier.stop
      run.catchup_poller.stop
      run.scheduled_message_poller.stop
      run.stale_claim_reaper.stop
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

      Run.new(
        work_queue:,
        workers:,
        catchup_poller: CatchUpPoller.new(
          work_queue:,
          reactors: @reactors,
          interval: @catchup_interval,
          logger: @logger
        ),
        scheduled_message_poller: ScheduledMessagePoller.new(
          store: @router.store,
          interval: @catchup_interval,
          logger: @logger
        ),
        stale_claim_reaper: StaleClaimReaper.new(
          store: @router.store,
          interval: @housekeeping_interval,
          ttl_seconds: @claim_ttl_seconds,
          worker_ids_provider: -> { workers.map(&:name) },
          logger: @logger
        )
      )
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
