# frozen_string_literal: true

require 'sourced/injector'

module Sourced
  module CCC
    class Router
      attr_reader :store, :reactors

      def initialize(store:)
        @store = store
        @reactors = []
        @needs_history = {}
      end

      def register(reactor_class)
        @reactors << reactor_class
        store.register_consumer_group(reactor_class.group_id)
        @needs_history[reactor_class] = Injector.resolve_args(reactor_class, :handle_batch).include?(:history)
      end

      def handle_next_for(reactor_class, worker_id: 'default')
        handled_types = reactor_class.handled_messages.map(&:type).uniq

        claim = store.claim_next(
          reactor_class.group_id,
          partition_by: reactor_class.partition_keys.map(&:to_s),
          handled_types: handled_types,
          worker_id: worker_id
        )
        return false unless claim

        begin
          kwargs = {}
          if @needs_history[reactor_class]
            attrs = claim.partition_value.transform_keys(&:to_sym)
            conditions = reactor_class.context_for(attrs)
            kwargs[:history] = store.read(conditions)
          end

          action_pairs = reactor_class.handle_batch(claim, **kwargs)

          if action_pairs == Actions::RETRY
            store.release(reactor_class.group_id, offset_id: claim.offset_id)
            return true
          end

          execute_actions(action_pairs, claim, reactor_class.group_id)
          true

        rescue Sourced::PartialBatchError => e
          execute_actions(e.action_pairs, claim, reactor_class.group_id)
          reactor_class.on_exception(e, e.failed_message, nil)
          true
        rescue Sourced::ConcurrentAppendError
          store.release(reactor_class.group_id, offset_id: claim.offset_id)
          true
        rescue StandardError => e
          store.release(reactor_class.group_id, offset_id: claim.offset_id)
          reactor_class.on_exception(e, claim.messages.first, nil)
          true
        end
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

      def execute_actions(action_pairs, claim, group_id)
        store.db.transaction do
          last_position = nil
          Array(action_pairs).each do |(actions, source_message)|
            Array(actions).each do |action|
              action.execute(store, source_message) unless action == Actions::OK
            end
            last_position = source_message.position if source_message.respond_to?(:position)
          end

          if last_position
            store.ack(group_id, offset_id: claim.offset_id, position: last_position)
          else
            store.release(group_id, offset_id: claim.offset_id)
          end
        end
      end
    end
  end
end
