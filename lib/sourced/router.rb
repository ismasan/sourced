# frozen_string_literal: true

require 'sourced/injector'

module Sourced
  class Router
    attr_reader :store, :reactors

    def initialize(store:)
      @store = store
      @reactors = []
      @needs_history = {}
      @action_runner = ActionRunner.new(store)
    end

    # Register a reactor. Reactors are duck-typed: only +handled_messages+ and
    # +handle_claim+ are required. Optional methods (+group_id+ — defaults to the
    # class name, +partition_keys+, +exclusive?+, +context_for+, lifecycle hooks)
    # get defaults so a plain class (e.g. a Sidereal Commander adapter) can
    # register without depending on Sourced.
    #
    # Deletion is never dictated by partitioning or exclusivity — it happens only
    # when a reactor's action carries +delete: true+ (see {ActionRunner}).
    # +exclusive+ governs routing (sole ownership of the handled types).
    # A reactor must declare +partition_by+; an +exclusive+ reactor may omit it to
    # get an id-partitioned queue (one partition per message).
    def register(reactor_class)
      ReactorDefaults.apply(reactor_class)
      exclusive = reactor_class.exclusive?
      partition_keys = effective_partition_keys(reactor_class)

      # id-partitioning (declared as `partition_by :__id`, or implied by omitting
      # partition_by) indexes messages by Message#id (key name "__id", chosen to
      # not collide with a payload attribute) and must be sole-owned, so it is only
      # allowed for exclusive reactors. This keeps id-indexing safe: a type is only
      # id-indexed when its lone exclusive owner is id-partitioned.
      if partition_keys == [:__id] && !exclusive
        raise ArgumentError,
          "#{reactor_class} must declare `partition_by`. (Only an `exclusive` reactor may " \
          "be id-partitioned — one partition per message — whether by omitting " \
          "partition_by or declaring `partition_by :__id`.)"
      end

      handled_types = reactor_class.handled_messages.map(&:type).uniq
      validate_exclusive_ownership!(reactor_class, handled_types, exclusive)

      @reactors << reactor_class

      store.register_consumer_group(
        reactor_class.group_id,
        partition_by: partition_keys.map(&:to_s),
        exclusive: exclusive,
        handled_types: handled_types
      )
      @needs_history[reactor_class] = Injector.resolve_args(reactor_class, :handle_claim).include?(:history)
    end

    def handle_next_for(reactor_class, worker_id: 'default', batch_size: nil)
      group_id = reactor_class.group_id
      handled_types = reactor_class.handled_messages.map(&:type).uniq

      claim = store.claim_next(
        group_id,
        partition_by: effective_partition_keys(reactor_class).map(&:to_s),
        handled_types: handled_types,
        worker_id: worker_id,
        batch_size: batch_size
      )
      return false unless claim

      begin
        kwargs = {}
        if @needs_history[reactor_class]
          attrs = claim.partition_value.transform_keys(&:to_sym)
          kwargs[:history] = store.read(reactor_class.context_for(attrs))
        end

        action_pairs = reactor_class.handle_claim(claim, **kwargs)

        if action_pairs == Actions::RETRY
          store.release(group_id, offset_id: claim.offset_id)
          return true
        end

        execute_actions(action_pairs, claim, reactor_class)
        true

      rescue Sourced::PartialBatchError => e
        execute_actions(e.action_pairs, claim, reactor_class)
        store.updating_consumer_group(group_id) do |group|
          reactor_class.on_exception(e, e.failed_message, group)
        end
        true
      rescue Sourced::ConcurrentAppendError
        store.release(group_id, offset_id: claim.offset_id)
        true
      rescue StandardError => e
        store.release(group_id, offset_id: claim.offset_id)
        store.updating_consumer_group(group_id) do |group|
          reactor_class.on_exception(e, claim.messages.first, group)
        end
        true
      end
    end

    # Stop a consumer group and invoke the reactor's {Consumer#on_stop} callback.
    #
    # Marks the group as stopped in the store so workers will no longer claim
    # work for it, then calls +on_stop+ on the reactor class.
    #
    # @param reactor_or_id [Class, String] a registered reactor class, or its +group_id+ string
    # @param message [String, nil] optional reason for stopping (persisted in the group's error_context)
    # @return [void]
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    #
    # @example Stop with a reactor class
    #   router.stop_consumer_group(CourseDecider, 'maintenance window')
    #
    # @example Stop with a string group_id
    #   router.stop_consumer_group('CourseDecider')
    def stop_consumer_group(reactor_or_id, message = nil)
      reactor = resolve_reactor_class(reactor_or_id)
      store.stop_consumer_group(reactor.group_id, message)
      reactor.on_stop(message)
    end

    # Reset a consumer group and invoke the reactor's {Consumer#on_reset} callback.
    #
    # Clears all partition offsets and resets the discovery position to 0,
    # so the group will reprocess messages from the beginning. Does not
    # change the group's status (a stopped group remains stopped after reset).
    # Then calls +on_reset+ on the reactor class.
    #
    # @param reactor_or_id [Class, String] a registered reactor class, or its +group_id+ string
    # @return [void]
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    #
    # @example
    #   router.reset_consumer_group(CourseDecider)
    def reset_consumer_group(reactor_or_id)
      reactor = resolve_reactor_class(reactor_or_id)
      store.reset_consumer_group(reactor.group_id)
      reactor.on_reset
    end

    # Start a consumer group and invoke the reactor's {Consumer#on_start} callback.
    #
    # Marks the group as active in the store so workers can claim work for it
    # again, then calls +on_start+ on the reactor class.
    #
    # @param reactor_or_id [Class, String] a registered reactor class, or its +group_id+ string
    # @return [void]
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    #
    # @example
    #   router.start_consumer_group(CourseDecider)
    def start_consumer_group(reactor_or_id)
      reactor = resolve_reactor_class(reactor_or_id)
      store.start_consumer_group(reactor.group_id)
      reactor.on_start
    end

    def drain(limit = Float::INFINITY)
      count = 0
      loop do
        count += 1
        found_any = @reactors.any? { |r| handle_next_for(r) }
        break unless found_any && count < limit
      end
    end

    private

    # A registered reactor's effective partition keys: its declared keys, or
    # +[:__id]+ (partition by Message#id) for an exclusive reactor that declared
    # none (validated in #register).
    def effective_partition_keys(reactor)
      keys = Array(reactor.partition_keys)
      keys.empty? ? [:__id] : keys
    end

    # Enforce that an exclusive reactor is the sole handler of its message types.
    # A message deleted once the exclusive reactor acks it must not be needed by
    # any other reactor.
    #
    # @raise [ArgumentError] on a message-type overlap involving an exclusive reactor
    def validate_exclusive_ownership!(reactor_class, handled_types, exclusive)
      @reactors.each do |existing|
        existing_exclusive = existing.exclusive?
        next unless exclusive || existing_exclusive

        overlap = handled_types & existing.handled_messages.map(&:type)
        next if overlap.empty?

        offender = exclusive ? reactor_class : existing
        raise ArgumentError, <<~MSG.strip
          Cannot register #{reactor_class}: exclusive reactor #{offender} requires sole ownership of its message types, but #{reactor_class} and #{existing} both handle: #{overlap.sort.join(', ')}
        MSG
      end
    end

    # Normalize a pair's +signals+ into an array, without Hash-splatting a single
    # Hash signal into pairs (which +Array()+ would do).
    def wrap_signals(signals)
      return [] if signals.nil?
      signals.is_a?(Array) ? signals : [signals]
    end

    # Resolve a reactor class or group_id string to a registered reactor class.
    #
    # @param reactor_or_id [Class, String] a reactor class (returned as-is) or a +group_id+ string
    # @return [Class] the matching registered reactor class
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    def resolve_reactor_class(reactor_or_id)
      return reactor_or_id if reactor_or_id.is_a?(Module)

      @reactors.find { |r| r.group_id == reactor_or_id } ||
        raise(ArgumentError, "No reactor registered with group_id '#{reactor_or_id}'")
    end

    # Interpret the reactor's returned action signals against the store, then
    # finalize the claim: advance the offset cursor and delete any messages the
    # reactor explicitly marked for deletion via a +delete: true+ signal.
    def execute_actions(action_pairs, claim, reactor)
      group_id = reactor.group_id
      interpreter = @action_runner
      after_sync_works = []

      store.db.transaction do
        last_position = nil
        flagged_deletes = []

        Array(action_pairs).each do |(signals, source_message)|
          delete_requested = false
          wrap_signals(signals).each do |signal|
            delete_requested = true if interpreter.run(signal, source_message, after_sync_works)
          end
          if source_message.respond_to?(:position)
            last_position = source_message.position
            flagged_deletes << source_message.position if delete_requested
          end
        end

        if last_position
          # Deletion is driven solely by :delete signals — never by the reactor's
          # partitioning or exclusivity.
          store.delete_messages(flagged_deletes) if flagged_deletes.any?
          store.ack(group_id, offset_id: claim.offset_id, position: last_position)
        else
          store.release(group_id, offset_id: claim.offset_id)
        end
      end

      after_sync_works.each(&:call)
    end
  end
end
