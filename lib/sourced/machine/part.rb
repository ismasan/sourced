# frozen_string_literal: true

module Sourced
  class Machine
    # A part of one machine's chart (its *owner*), defined in its own class so
    # a big machine can be split up. A part has the chart DSL ({Definition}),
    # and the owner's partition, messages and context; it's mounted only in its
    # owner's chart (+state X, Part+ or +region :x, Part+), and never registered,
    # instantiated or run on its own. It isn't a {Machine}: it has none of a
    # machine's own partition, consumer group, context, reactions or sync.
    #
    #   class Payroll
    #     class PendingMachine < Sourced::Machine::Part
    #       part_of Payroll
    #
    #       Queued = State(:queued)       # payroll.runtime.pending.queued
    #       Process = Command(:process)   # payroll.pending.process, with Payroll's keys first
    #       # …
    #     end
    #   end
    #
    # +part_of+ another part binds the part to that part's owner, and nests
    # its prefix under that part's.
    class Part
      extend Definition

      private_class_method :new

      # @return [Class<Machine>, nil] the machine whose chart this is part of
      def self.owner = @owner

      # Declare the machine this is a part of, before anything else. A part
      # given binds this part to its owner, and nests this part's prefix in its.
      #
      # @param owner [Class<Machine>, Class<Part>]
      # @param prefix [String, nil] the part's segment of its types (see {.part_prefix});
      #   required for an anonymous part
      def self.part_of(owner, prefix: nil)
        raise DefinitionError, "#{self}: part_of is declared twice" if @owner
        if declared_states.any? || root.nodes.any? || @__messages_defined
          raise DefinitionError, "#{self}: declare part_of first"
        end
        raise DefinitionError, "#{self}: part_of prefix: can't be blank" if prefix && prefix.to_s.strip.empty?
        unless Class === owner && (owner < Machine || owner < Part)
          raise DefinitionError, "#{self}: part_of takes a Sourced::Machine or a part, got #{owner.inspect}"
        end
        raise DefinitionError, "#{self}: a part can't be part of itself" if owner <= self

        if owner < Part
          raise DefinitionError, "#{self}: #{owner} has no part_of, so it's part of nothing yet" unless owner.owner

          @enclosing = owner
          owner = owner.owner
        end
        @owner = owner
        @part_prefix = prefix&.to_s
      end

      # The part's segment of its message and state types: +part_of+'s
      # +prefix:+, or its name relative to its owner (or the part it's
      # +part_of+), underscored, without a trailing +Machine+
      # (+Payroll::PendingMachine+ => +pending+), after the enclosing part's
      # prefix when there is one.
      #
      # @return [String]
      def self.part_prefix = @part_prefix ||= __default_part_prefix

      # +<owner's message_prefix>.<part_prefix>+, which {.Command} and {.Event} type messages with.
      def self.message_prefix = "#{__owner!.message_prefix}.#{part_prefix}"

      def self.type_prefix = message_prefix
      def self.partition_schema = __owner!.partition_schema
      def self.partition_keys = __owner!.partition_keys

      # A command +<message_prefix>.<name>+, declaring the owner's partition keys first.
      def self.Command(name, &block) = __define_message(Sourced::Command, name, &block)

      # An event +<message_prefix>.<name>+, declaring the owner's partition keys first.
      def self.Event(name, &block) = __define_message(Sourced::Event, name, &block)

      # Evolve the owner's context with +message_class+, as the owner's own
      # +evolve+ blocks do. One block per message class across the owner and
      # its parts (checked when the owner is validated, which locks it).
      #
      # @param message_class [Class<Sourced::Message>]
      # @yield [context, message]
      def self.evolve(message_class, &block)
        __owner!
        raise DefinitionError, "#{self}: evolve declared after #{owner} was validated" if @__locked
        raise DefinitionError, "#{self}: evolve #{message_class} needs a block" unless block

        projection_class.evolve(message_class, &block)
      end

      # The part's evolve blocks, which its owner runs on its context.
      # @api private
      def self.projection_class = @projection_class ||= Class.new(Sourced::Projection)

      # Lock the part's DSL once its owner has read it.
      # @api private
      def self.__lock! = @__locked = true

      # @api private
      def self.__type_owner_key = [__owner!.group_id, part_prefix]

      private_class_method def self.__owner!
        @owner || raise(DefinitionError, "#{self}: declare part_of first")
      end

      private_class_method def self.__chart_owner = __owner!

      private_class_method def self.__default_part_prefix
        raise DefinitionError, "#{self}: an anonymous part needs part_of's prefix:" unless name

        scope = [@enclosing, __owner!].find { |mod| mod&.name && name.start_with?("#{mod.name}::") }
        relative = scope ? name.delete_prefix("#{scope.name}::") : name
        relative = Types::ModuleToMessageType.parse(relative.sub(/(?<=.)Machine\z/, ''))
        @enclosing ? "#{@enclosing.part_prefix}.#{relative}" : relative
      end

      private_class_method def self.__state_type(name) = "#{__owner!.message_prefix}.runtime.#{part_prefix}.#{name}"

      private_class_method def self.__define_message(base, name, &block)
        if __owner!.partition_keys.empty?
          raise DefinitionError, "#{self}: declare partition_by on #{owner} before defining its parts' messages"
        end

        schema = partition_schema
        @__messages_defined = true
        base.define("#{message_prefix}.#{name}") do
          schema.to_h.each { |key, key_type| attribute key.to_sym, key_type }
          class_exec(&block) if block
        end
      end

      private_class_method def self.__define_node(...)
        __owner!
        super
      end
    end
  end
end
