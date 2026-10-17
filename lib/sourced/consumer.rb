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
    # Copy the partition schema into subclasses, which can redeclare it.
    #
    # @param subclass [Class] subclass being created
    # @return [void]
    def inherited(subclass)
      super
      subclass.partition_by(**@partition_schema.to_h.transform_keys(&:to_sym)) if @partition_schema
    end

    # @return [Plumb::HashClass] the partition keys and their types
    def partition_schema
      @partition_schema ||= Types::Hash
    end

    def partition_keys
      @partition_keys ||= partition_schema.to_h.keys.map(&:to_sym).freeze
    end

    # Declare the partition keys. Bare keys are +Types::String+; give a type
    # with a Hash:
    #
    #   partition_by :order_id, item_id: Types::UUID::V4
    #
    # Messages handled by the reactor are checked against it (see {PartitionSchema}).
    def partition_by(*keys, **typed)
      __check_no_messages_defined!(:partition_by)
      schema = keys.flatten.to_h { |key| [key.to_sym, Types::String] }
      typed.each do |key, type|
        raise ArgumentError, "#{self}: partition key #{key} is declared twice" if schema.key?(key.to_sym)
        unless Plumb::Composable === type || Class === type
          raise ArgumentError, "#{self}: partition key #{key} needs a Plumb type or a class, got #{type.inspect}"
        end

        schema[key.to_sym] = Plumb::Composable.wrap(type)
      end
      @partition_keys = nil
      @partition_schema = Types::Hash[schema]
    end

    def group_id
      @group_id ||= name
    end

    def consumer_group(id)
      __check_no_messages_defined!(:consumer_group)
      @group_id = id
    end

    # The prefix of the types {.Command} and {.Event} define. Defaults to
    # +group_id+ (+Shop::Orders+ => +shop.orders+); declare one to keep stored
    # types stable when the class or group is renamed.
    #
    # @param prefix [String, nil] the prefix to declare, or nil to read it
    # @return [String]
    def message_prefix(prefix = nil)
      if prefix
        __check_no_messages_defined!(:message_prefix)
        return @message_prefix = prefix.to_s
      end
      @message_prefix || Types::ModuleToMessageType.parse(group_id.to_s)
    end

    # Define a command typed +"<message_prefix>.<name>"+, with the partition
    # schema's attributes before the block's.
    #
    #   AddItem = Command(:add_item) do
    #     attribute :quantity, Integer
    #   end
    #
    # @return [Class<Sourced::Command>]
    def Command(name, &block) = __define_message(Sourced::Command, name, &block)

    # Define an event, as {.Command} does.
    # @return [Class<Sourced::Event>]
    def Event(name, &block) = __define_message(Sourced::Event, name, &block)

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

    # Build query conditions from partition attributes and handled evolve types.
    # Override in reactor for custom per-command conditions.
    def context_for(partition_attrs)
      handled_messages_for_evolve.flat_map { |klass|
        klass.to_conditions(**partition_attrs)
      }
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

    private def __define_message(base, name, &block)
      prefix = message_prefix
      raise ArgumentError, "#{self}: defining #{name} needs a group_id, or declare message_prefix" if prefix.empty?

      schema = partition_schema
      @__messages_defined = true
      base.define("#{prefix}.#{name}") do
        schema.to_h.each { |key, key_type| attribute key.to_sym, key_type }
        class_exec(&block) if block
      end
    end

    # Messages take the schema and prefix when they're defined.
    private def __check_no_messages_defined!(what)
      raise ArgumentError, "#{self}: declare #{what} before defining messages with Command or Event" if @__messages_defined
    end
  end
end
