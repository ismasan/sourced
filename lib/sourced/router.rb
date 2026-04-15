# frozen_string_literal: true

require 'sourced/injector'

module Sourced
  class Router
    attr_reader :store, :reactors

    def initialize(store:)
      @store = store
      @reactors = []
      @needs_history = {}
    end

    def register(reactor_class)
      @reactors << reactor_class
      store.register_consumer_group(
        reactor_class.group_id,
        partition_by: reactor_class.partition_keys.map(&:to_s)
      )
      @needs_history[reactor_class] = Injector.resolve_args(reactor_class, :handle_claim).include?(:history)
    end

    def handle_next_for(reactor_class, worker_id: 'default', batch_size: nil)
      handled_types = reactor_class.handled_messages.map(&:type).uniq

      claim = store.claim_next(
        reactor_class.group_id,
        partition_by: reactor_class.partition_keys.map(&:to_s),
        handled_types: handled_types,
        worker_id: worker_id,
        batch_size: batch_size
      )
      return false unless claim

      begin
        kwargs = {}
        if @needs_history[reactor_class]
          attrs = claim.partition_value.transform_keys(&:to_sym)
          conditions = reactor_class.context_for(attrs)
          kwargs[:history] = store.read(conditions)
        end

        action_pairs = reactor_class.handle_claim(claim, **kwargs)

        if action_pairs == Actions::RETRY
          store.release(reactor_class.group_id, offset_id: claim.offset_id)
          return true
        end

        execute_actions(action_pairs, claim, reactor_class.group_id)
        true

      rescue Sourced::PartialBatchError => e
        execute_actions(e.action_pairs, claim, reactor_class.group_id)
        store.updating_consumer_group(reactor_class.group_id) do |group|
          reactor_class.on_exception(e, e.failed_message, group)
        end
        true
      rescue Sourced::ConcurrentAppendError
        store.release(reactor_class.group_id, offset_id: claim.offset_id)
        true
      rescue StandardError => e
        store.release(reactor_class.group_id, offset_id: claim.offset_id)
        store.updating_consumer_group(reactor_class.group_id) do |group|
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
      reactor_class = resolve_reactor_class(reactor_or_id)
      store.stop_consumer_group(reactor_class.group_id, message)
      reactor_class.on_stop(message)
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
      reactor_class = resolve_reactor_class(reactor_or_id)
      store.reset_consumer_group(reactor_class.group_id)
      reactor_class.on_reset
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
      reactor_class = resolve_reactor_class(reactor_or_id)
      store.start_consumer_group(reactor_class.group_id)
      reactor_class.on_start
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

    def execute_actions(action_pairs, claim, group_id)
      after_sync_actions = []

      store.db.transaction do
        last_position = nil
        Array(action_pairs).each do |(actions, source_message)|
          Array(actions).each do |action|
            if action.is_a?(Actions::AfterSync)
              after_sync_actions << action
            elsif action != Actions::OK
              action.execute(store, source_message)
            end
          end
          last_position = source_message.position if source_message.respond_to?(:position)
        end

        if last_position
          store.ack(group_id, offset_id: claim.offset_id, position: last_position)
        else
          store.release(group_id, offset_id: claim.offset_id)
        end
      end

      after_sync_actions.each(&:call)
    end
  end
end
