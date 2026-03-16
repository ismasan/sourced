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
        store.register_consumer_group(
          reactor_class.group_id,
          partition_by: reactor_class.partition_keys.map(&:to_s)
        )
        @needs_history[reactor_class] = Injector.resolve_args(reactor_class, :handle_claim).include?(:history)
      end

      def handle_next_for(reactor_class, worker_id: 'default', batch_size: nil)
        handled_types = reactor_class.handled_messages.map(&:type).uniq

        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        claim = store.claim_next(
          reactor_class.group_id,
          partition_by: reactor_class.partition_keys.map(&:to_s),
          handled_types: handled_types,
          worker_id: worker_id,
          batch_size: batch_size
        )
        t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Console.info "AAA #{reactor_class.name} claim_next=#{((t1-t0)*1000).round(1)}ms found=#{!!claim}"
        return false unless claim

        begin
          kwargs = {}
          if @needs_history[reactor_class]
            attrs = claim.partition_value.transform_keys(&:to_sym)
            conditions = reactor_class.context_for(attrs)
            t2 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            kwargs[:history] = store.read(conditions)
            t3 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            Console.info "AAA #{reactor_class.name} read_history=#{((t3-t2)*1000).round(1)}ms"
          end

          t4 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          action_pairs = reactor_class.handle_claim(claim, **kwargs)
          t5 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          Console.info "AAA #{reactor_class.name} handle_claim=#{((t5-t4)*1000).round(1)}ms"

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
        after_sync_actions = []

        t_tx0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
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

        t_tx1 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Console.info "AAA transaction=#{((t_tx1-t_tx0)*1000).round(1)}ms"
        after_sync_actions.each(&:call)
      end
    end
  end
end
