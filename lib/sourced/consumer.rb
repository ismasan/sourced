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
    def self.extended(base)
      super
      base.extend ClassMethods
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

    # Message types this consumer evolves from. Used by {#context_for}
    # to build query conditions for history reads.
    # Defaults to empty; overridden by Sourced::Evolve mixin.
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
      Sourced.config.error_strategy.call(exception, message, group)
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

    module ClassMethods
      # Resolve a messages class from a symbol or type-like string.
      #
      # Symbols are normalized by replacing dots with underscores before
      # matching against registered message types. For example,
      # +:course_created+ matches <tt>"course.created"</tt> and
      # <tt>"course_created"</tt>.
      #
      # @param message_symbol [Symbol, String] symbolic message identifier
      # @return [Class, nil] matching messages class, or +nil+ if none found
      #
      # @example
      #   CourseDecider[:courses_created]
      #   # => CourseCreated
      def [](message_symbol)
        normalized = message_symbol.to_s.tr('.', '_')
        find_registered_message_class(normalized)
      end

      private

      def find_registered_message_class(normalized_name, base = Sourced::Message)
        base.registry.keys.each do |type|
          klass = base.registry[type]
          return klass if type.tr('.', '_') == normalized_name
        end

        base.subclasses.each do |subclass|
          klass = find_registered_message_class(normalized_name, subclass)
          return klass if klass
        end

        nil
      end
    end
  end
end
