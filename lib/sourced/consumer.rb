# frozen_string_literal: true

module Sourced
  # A consumer group row's changes, as {ConsumerGroupMachine} decides them:
  # placed at the row's status and context, each method moves the machine
  # with a command. Operator commands ({#stop}, {#start}, {#reset}) raise
  # {Store::InvalidConsumerGroupTransitionError} where the status doesn't
  # accept them. Worker commands ({#retry}, {#fail}) come from error
  # handling, and are ignored there. Used by {Store#updating_consumer_group}.
  class GroupUpdater
    attr_reader :group_id

    # @param group_id [String]
    # @param row [Hash] the group's row, its +error_context+ JSON-native (parsed) or nil
    # @param logger [Logger]
    # @param offset_id [Integer, nil] the claim being processed, which {#retry} records
    def initialize(group_id, row, logger, offset_id: nil)
      @group_id = group_id
      @logger = logger
      @offset_id = offset_id
      context = ConsumerGroupMachine.decode_context(row[:error_context] || {})
      @machine = ConsumerGroupMachine.at([row[:status]], context, group_id:)
      @columns = nil
    end

    # @return [String] the group's status: its machine's path
    def status = @machine.machine.paths.first

    # @return [Hash] the group's {ConsumerGroupMachine::Context}
    def error_context = @machine.context

    # The columns to write, with the row's new status and context: none when
    # the machine didn't move.
    # @return [Hash]
    def updates
      return {} unless @columns

      @columns.merge(
        status:,
        error_context: JSON.dump(ConsumerGroupMachine.encode_context(error_context)),
        retry_at: error_context[:retry_at]&.iso8601,
        updated_at: Time.now.iso8601
      )
    end

    # @param message [String, nil] the operator's reason
    def stop(message: nil)
      @logger.error "Sourced: stopping consumer group #{group_id}"
      move!(ConsumerGroupMachine::Stop, message:)
    end

    # Resume a stopped or failed group, or a retrying one without waiting.
    def start
      move!(ConsumerGroupMachine::Start)
      # Reset the scan latch/watermark so the group rescans on resume.
      # 0 is "dirty" for partitioned groups and "no latch" for legacy ones.
      @columns[:last_nil_types_max_pos] = 0
    end

    # Reset a stopped or failed group's discovery; the store deletes its offsets.
    def reset
      move!(ConsumerGroupMachine::Reset)
      @columns.merge!(discovery_position: 0, last_nil_types_max_pos: 0)
    end

    # Try the group again at +time+, recording +exception+ and the claim that failed.
    # @param time [Time]
    # @param exception [Exception, nil]
    def retry(time, exception: nil)
      @logger.warn "Sourced: retrying consumer group #{group_id} at #{time}"
      move(ConsumerGroupMachine::Retry, retry_at: time, offset_id: @offset_id, **failure(exception))
    end

    # @param exception [Exception, nil] recorded with its backtrace
    def fail(exception: nil)
      @logger.error "Sourced: failing consumer group #{group_id}. #{exception&.class}: #{exception&.message}"
      move(ConsumerGroupMachine::Fail, **failure(exception), exception_backtrace: exception&.backtrace)
    end

    private

    def failure(exception)
      exception ? { exception_class: exception.class.to_s, exception_message: exception.message } : {}
    end

    # An operator command: raise where the status doesn't accept it.
    def move!(command_class, **payload)
      command = command_for(command_class, payload)
      unless @machine.handles_command?(command)
        available = @machine.available_commands.to_a & ConsumerGroupMachine::OPERATOR_COMMANDS
        raise Store::InvalidConsumerGroupTransitionError.new(group_id, status, command_class, available)
      end

      decide(command)
    end

    # A worker command: ignored where the status doesn't accept it.
    def move(command_class, **payload)
      command = command_for(command_class, payload)
      if @machine.handles_command?(command)
        decide(command)
      else
        @logger.warn "Sourced: consumer group #{group_id} is #{status}, ignoring #{command_class.type}"
      end
    end

    def command_for(command_class, payload)
      command = command_class.new(payload: { group_id:, **payload })
      raise ArgumentError, "invalid #{command_class}: #{command.errors.inspect}" unless command.valid?

      command
    end

    def decide(command)
      @machine.decide(command)
      @columns ||= {}
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

    # The consumer group id: the declared {.consumer_group}, or else the class
    # name, normalized by {Types::GroupId} (+CrossPost::Account+ =>
    # +cross_post.account+). Nil for an anonymous class with no declared group.
    def group_id
      @group_id ||= name && Types::GroupId.parse(name)
    end

    # The name the group is shown under: the class name, which locates the
    # reactor in code whatever its {.group_id}. Falls back to the group id for
    # an anonymous class.
    def group_label = name || group_id

    # Declare the consumer group id. Normalized by {Types::GroupId}, so
    # +'Shop::Orders'+ and +'shop.orders'+ declare the same group.
    #
    # @raise [ArgumentError] if +id+ can't be normalized into a group id
    def consumer_group(id)
      __check_no_messages_defined!(:consumer_group)
      result = Types::GroupId.resolve(id)
      raise ArgumentError, "#{self}: invalid consumer_group #{id.inspect}: #{result.errors}" unless result.valid?

      @group_id = result.value
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
