# frozen_string_literal: true

require 'sourced/work_queue'
require 'sourced/catchup_poller'
require 'sourced/ccc/worker'
require 'sourced/ccc/stale_claim_reaper'

module Sourced
  module CCC
    # Orchestrator that wires together the signal-driven dispatch pipeline for CCC:
    # {WorkQueue}, {NotificationQueuer}, {CatchUpPoller}, store notifier, and CCC {Worker}s.
    #
    # Mirrors {Sourced::Dispatcher} but uses CCC-specific interfaces:
    # - +router.reactors+ instead of +router.async_reactors+
    # - +reactor.group_id+ instead of +reactor.consumer_info.group_id+
    # - +router.store.notifier+ instead of +router.backend.notifier+
    #
    # Does not own the process lifecycle — the caller provides the task/fiber
    # context via {#spawn_into}, and triggers shutdown via {#stop}.
    #
    # @example Usage with a task runner
    #   dispatcher = CCC::Dispatcher.new(router: ccc_router, worker_count: 4)
    #   executor.start do |task|
    #     dispatcher.spawn_into(task)
    #   end
    #   dispatcher.stop
    #
    # @example With custom queue for testing
    #   queue = WorkQueue.new(max_per_reactor: 2, queue: Queue.new)
    #   dispatcher = CCC::Dispatcher.new(router: ccc_router, work_queue: queue)
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

      # @return [Array<CCC::Worker>] worker instances managed by this dispatcher
      attr_reader :workers

      # @param router [CCC::Router] the CCC router providing reactors and store
      # @param worker_count [Integer] number of worker fibers to spawn (default 2)
      # @param batch_size [Integer] max messages per claim (default 50)
      # @param max_drain_rounds [Integer] max drain iterations before re-enqueue (default 10)
      # @param catchup_interval [Numeric] seconds between catch-up polls (default 5)
      # @param housekeeping_interval [Numeric] seconds between heartbeat/reap cycles (default 30)
      # @param claim_ttl_seconds [Integer] stale claim age threshold in seconds (default 120)
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
        work_queue: nil,
        logger: CCC.config.logger
      )
        @logger = logger
        @router = router
        @workers = []

        return if worker_count.zero?

        reactors = router.reactors.select { |r| r.handled_messages.any? }.to_a

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

        notification_queuer = NotificationQueuer.new(work_queue: @work_queue, reactors: reactors)
        @store_notifier = router.store.notifier
        @store_notifier.subscribe(notification_queuer)

        @catchup_poller = CatchUpPoller.new(
          work_queue: @work_queue,
          reactors: reactors,
          interval: catchup_interval,
          logger: logger
        )

        @stale_claim_reaper = StaleClaimReaper.new(
          store: router.store,
          interval: housekeeping_interval,
          ttl_seconds: claim_ttl_seconds,
          worker_ids_provider: -> { @workers.map(&:name) },
          logger: logger
        )
      end

      # Spawn all component fibers into the caller's task context.
      # Spawns: store notifier (e.g. PG LISTEN), catch-up poller, and N workers.
      #
      # @param task [Object] an executor task or Async::Task to spawn fibers into
      # @return [void]
      def spawn_into(task)
        return if @workers.empty?

        s = task.respond_to?(:spawn) ? :spawn : :async

        # Store notifier (start — no-op for InlineNotifier)
        task.send(s) { @store_notifier.start }

        # CatchUp poller
        task.send(s) { @catchup_poller.run }

        # Stale claim reaper
        task.send(s) { @stale_claim_reaper.run }

        # Workers
        @workers.each do |w|
          task.send(s) { w.run }
        end
      end

      # Stop all components and close the work queue.
      #
      # @return [void]
      def stop
        return if @workers.empty?

        @logger.info "CCC::Dispatcher: stopping #{@workers.size} workers"
        @store_notifier.stop
        @catchup_poller.stop
        @stale_claim_reaper.stop
        @workers.each(&:stop)
        @work_queue.close(@workers.size)
        @logger.info 'CCC::Dispatcher: all components stopped'
      end
    end
  end
end
