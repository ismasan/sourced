# frozen_string_literal: true

module Sourced
  # Accumulates mutations to a consumer group row for atomic persistence.
  # Accumulates consumer group mutations for atomic persistence.
  # Used by {Store#updating_consumer_group}.
  class GroupUpdater
    attr_reader :group_id, :updates, :error_context

    def initialize(group_id, row, logger)
      @group_id = group_id
      @logger = logger
      @error_context = row[:error_context]
      @updates = { error_context: @error_context.dup }
    end

    def stop(message: nil)
      @logger.error "Sourced: stopping consumer group #{group_id}"
      @updates[:status] = Store::STOPPED
      @updates[:retry_at] = nil
      @updates[:updated_at] = Time.now.iso8601
      @updates[:error_context][:message] = message if message
    end

    def fail(exception: nil)
      @logger.error "Sourced: failing consumer group #{group_id}. #{exception&.class}: #{exception&.message}"
      @updates[:status] = Store::FAILED
      @updates[:retry_at] = nil
      @updates[:updated_at] = Time.now.iso8601
      if exception
        @updates[:error_context][:exception_class] = exception.class.to_s
        @updates[:error_context][:exception_message] = exception.message
      end
    end

    def retry(time, **ctx)
      @logger.warn "Sourced: retrying consumer group #{group_id} at #{time}"
      @updates[:updated_at] = Time.now.iso8601
      @updates[:retry_at] = time.iso8601
      @updates[:error_context].merge!(ctx)
    end
  end

  # Shared consumer configuration for reactors.
  # Extended (not included) onto reactor classes.
  module Consumer
    # Copy partition keys into subclasses, which can redeclare them.
    #
    # @param subclass [Class] subclass being created
    # @return [void]
    def inherited(subclass)
      super
      subclass.partition_by(@partition_keys) if @partition_keys
    end

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

    # Declare this reactor as the exclusive owner of its handled message types:
    # the router validates that no other reactor handles the same types. This is
    # a routing concern only — it does not delete messages. Deletion happens
    # solely when an action carries +delete: true+.
    #
    # Declaring +exclusive+ also lets a reactor omit +partition_by+ to become an
    # id-partitioned queue (one partition per message). Reactors that delete
    # their handled messages should be exclusive so no other reactor is starved.
    #
    # @param value [Boolean]
    # @return [void]
    def exclusive(value = true)
      @exclusive = value
    end

    # @return [Boolean] whether this reactor exclusively owns its message types
    def exclusive?
      @exclusive ||= false
    end

    # Message types this consumer evolves from. Used by {#context_for}
    # to build query conditions for history reads.
    # Defaults to empty; overridden by Sourced::Evolve mixin.
    def handled_messages_for_evolve
      @handled_messages_for_evolve ||= []
    end

    # The history query {Router} reads for a claimed partition: a
    # {PartitionQuery} over the evolve types, matching messages as the claim
    # does. Empty (no history) when the reactor evolves nothing or is
    # id-partitioned. Override to return custom +QueryCondition+s instead.
    #
    # @param partition_attrs [Hash{Symbol => String}]
    # @return [PartitionQuery, Array<QueryCondition>]
    def context_for(partition_attrs)
      return Plumb::BLANK_ARRAY if handled_messages_for_evolve.empty? || IdPartition.keys?(partition_attrs.keys)

      PartitionQuery.new(attrs: partition_attrs, types: handled_messages_for_evolve.map(&:type))
    end

    # Called by {Router#stop_consumer_group} after the group is marked as stopped.
    # Override in reactor classes to run cleanup logic on stop.
    #
    # @param message [String, nil] optional reason for stopping
    # @return [void]
    def on_stop(message = nil)
      # no-op by default
    end

    # Called by {Router#reset_consumer_group} after the group's offsets are cleared.
    # Override in reactor classes to run cleanup logic on reset
    # (e.g. clearing caches or projections).
    #
    # @return [void]
    def on_reset
      # no-op by default
    end

    # Called by {Router#start_consumer_group} after the group is marked as active.
    # Override in reactor classes to run setup logic on start.
    #
    # @return [void]
    def on_start
      # no-op by default
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
