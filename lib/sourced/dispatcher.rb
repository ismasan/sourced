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
  # context via {#start}, and triggers shutdown via {#stop}.
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

    # @return [Array<Sourced::Worker>] worker instances managed by this dispatcher
    attr_reader :workers

    # @param router [Sourced::Router] the router providing reactors and store
    # @param worker_count [Integer] number of worker fibers to spawn (default 2)
    # @param batch_size [Integer] max messages per claim (default 50)
    # @param max_drain_rounds [Integer] max drain iterations before re-enqueue (default 10)
    # @param catchup_interval [Numeric] seconds between catch-up polls (default 5)
    # @param housekeeping_interval [Numeric] seconds between heartbeat/reap cycles (default 30)
    # @param claim_ttl_seconds [Integer] stale claim age threshold in seconds (default 120)
    # @param shutdown_timeout [Numeric] seconds {#stop} waits for workers to finish
    #   the batches they're processing (default 30)
    # @param work_queue [WorkQueue, nil] optional pre-built queue (useful for testing)
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
      @shutdown_timeout = shutdown_timeout
      @workers = []

      return if worker_count.zero?

      reactors = router.reactors.select { |r| r.handled_messages.any? }.to_a

      @work_queue = work_queue || WorkQueue.new(max_per_reactor: worker_count)

      @workers = worker_count.times.map do |i|
        Worker.new(
          work_queue: @work_queue,
          router:,
          name: "worker-#{i}",
          batch_size:,
          max_drain_rounds:,
          logger:
        )
      end

      notification_queuer = NotificationQueuer.new(work_queue: @work_queue, reactors: reactors)
      @store_notifier = router.store.notifier
      @store_notifier.subscribe(notification_queuer)

      @catchup_poller = CatchUpPoller.new(
        work_queue: @work_queue,
        reactors:,
        interval: catchup_interval,
        logger:
      )

      @scheduled_message_poller = ScheduledMessagePoller.new(
        store: router.store,
        interval: catchup_interval,
        logger:
      )

      @stale_claim_reaper = StaleClaimReaper.new(
        store: router.store,
        interval: housekeeping_interval,
        ttl_seconds: claim_ttl_seconds,
        worker_ids_provider: -> { @workers.map(&:name) },
        logger:
      )
    end

    # Spawn all component fibers into the caller's task context.
    # Spawns: store notifier (e.g. PG LISTEN), catch-up poller, and N workers.
    #
    # @param task [Object] an executor task or Async::Task to spawn fibers into
    # @return [void]
    # @raise [ArgumentError] if there are workers to run and +task+ can't spawn them
    def start(task)
      return if @workers.empty?

      s = %i[spawn async].find { |m| task.respond_to?(m) }
      unless s
        raise ArgumentError, "can't spawn #{@workers.size} workers into #{task.inspect}: " \
                             'start with an Async::Task, or Sourced::ThreadExecutor.new to run them in threads, ' \
                             'or set workers.count to 0 to run no workers in this process'
      end

      # Store notifier (start — no-op for InlineNotifier)
      task.send(s) { @store_notifier.start }

      # CatchUp poller
      task.send(s) { @catchup_poller.run }

      # Scheduled message poller
      task.send(s) { @scheduled_message_poller.run }

      # Stale claim reaper
      task.send(s) { @stale_claim_reaper.run }

      # Workers
      @workers.each do |w|
        task.send(s) { w.run }
      end

      self
    end

    # Stop all components, close the work queue, and wait for workers to finish
    # the batches they're processing, for up to +shutdown_timeout+ seconds in all.
    # Workers that haven't started running aren't waited for.
    #
    # Waiting blocks the caller's thread, or yields to the fiber scheduler when
    # called from a fiber (ex. an Async task), so the workers can finish.
    #
    # @return [Boolean] true if every worker finished, false if the timeout expired first
    def stop
      return true if @workers.empty?

      @logger.info "Sourced::Dispatcher: stopping #{@workers.size} workers"
      @store_notifier.stop
      @catchup_poller.stop
      @scheduled_message_poller.stop
      @stale_claim_reaper.stop
      @workers.each(&:stop)
      @work_queue.close(@workers.size)

      unfinished = wait_for_workers
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

      names = @workers.select(&:running?).map(&:name).join(', ')
      raise ShutdownTimeoutError, "workers still running after #{@shutdown_timeout}s: #{names}"
    end

    private

    # @return [Array<Worker>] workers still running when the timeout expired
    def wait_for_workers
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @shutdown_timeout
      @workers.reject do |worker|
        remaining = [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
        worker.wait(timeout: remaining)
      end
    end
  end
end
