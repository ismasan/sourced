# frozen_string_literal: true

module Sourced
  module CCC
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

      # Build query conditions from partition attributes and handled evolve types.
      # Override in reactor for custom per-command conditions.
      def context_for(partition_attrs)
        handled_messages_for_evolve.flat_map { |klass|
          klass.to_conditions(**partition_attrs)
        }
      end

      def on_exception(exception, message, group)
        Sourced.config.error_strategy.call(exception, message, group)
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
