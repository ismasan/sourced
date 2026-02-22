# frozen_string_literal: true

module Sourced
  module CCC
    # Accumulates mutations to a consumer group row for atomic persistence.
    # Mirrors Sourced::Backends::SequelBackend::GroupUpdater.
    # Used by {Store#updating_consumer_group}.
    class GroupUpdater
      attr_reader :group_id, :updates, :error_context

      def initialize(group_id, row, logger)
        @group_id = group_id
        @logger = logger
        @error_context = row[:error_context]
        @updates = { error_context: @error_context.dup }
      end

      def stop(exception:, message:)
        @logger.error "CCC: stopping consumer group #{group_id} message: '#{message&.type}' (#{message&.id}). #{exception&.class}: #{exception&.message}"
        @updates[:status] = Store::STOPPED
        @updates[:retry_at] = nil
        @updates[:updated_at] = Time.now.iso8601
      end

      def retry(time, **ctx)
        @logger.warn "CCC: retrying consumer group #{group_id} at #{time}"
        @updates[:updated_at] = Time.now.iso8601
        @updates[:retry_at] = time.iso8601
        @updates[:error_context].merge!(ctx)
      end
    end

    # Shared consumer configuration for CCC reactors.
    # Extended (not included) onto reactor classes.
    module Consumer
      def partition_keys
        @partition_keys ||= []
      end

      def partition_by(*keys)
        @partition_keys = keys.flatten.map(&:to_sym)
      end

      def group_id
        @group_id ||= name
      end

      def consumer_group(id)
        @group_id = id
      end

      # Message types this consumer evolves from. Used by {#context_for}
      # to build query conditions for history reads.
      # Defaults to empty; overridden by CCC::Evolve mixin.
      def handled_messages_for_evolve
        @handled_messages_for_evolve ||= []
      end

      # Build query conditions from partition attributes and handled evolve types.
      # Override in reactor for custom per-command conditions.
      def context_for(partition_attrs)
        handled_messages_for_evolve.flat_map { |klass|
          klass.to_conditions(**partition_attrs)
        }
      end

      def on_exception(exception, message, group)
        CCC.config.error_strategy.call(exception, message, group)
      end

      # Iterate messages collecting [actions, message] pairs.
      # On mid-batch failure, raises PartialBatchError with pairs collected so far.
      # If the first message fails, re-raises the original error.
      def each_with_partial_ack(messages)
        results = []
        messages.each do |msg|
          pair = yield(msg)
          results << pair if pair
        rescue StandardError => e
          raise e if results.empty?
          raise Sourced::PartialBatchError.new(results, msg, e)
        end
        results
      end
    end
  end
end
