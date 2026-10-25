# frozen_string_literal: true

module Sourced
  class Machine
    # A part of one machine's chart (its *owner*), defined in its own class so
    # a big machine can be split up. A part has the chart DSL ({Definition}),
    # and the owner's partition, messages and context; it's mounted only in its
    # owner's chart (+state Part+ or +region :x, Part+), and never registered,
    # instantiated or run on its own. It isn't a {Machine}: it has none of a
    # machine's own partition, consumer group, context, reactions or sync.
    #
    # A part is its own compound state, +Part::State+ ({.to_state_handle}): +state Part+
    # places it, mounting the part, and the part class stands for it wherever
    # a state is taken (targets, +to+, +on_final+, +.history+).
    #
    #   class Payroll
    #     class PendingMachine < Sourced::Machine::Part
    #       part_of Payroll               # State: payroll.runtime.pending
    #
    #       Queued = State(:queued)       # payroll.runtime.pending.queued
    #       Process = Command(:process)   # payroll.pending.process, with Payroll's keys first
    #       # …
    #     end
    #
    #     state PendingMachine do
    #       on_final PendingMachine::Done => Paid
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

      # The part's compound state, +Part::State+: typed
      # +<owner's message_prefix>.runtime.<part_prefix>+, with the owner's
      # partition keys, and named (in paths) after +part_prefix+'s last segment.
      # Its +machine+ is the part; the machine placing it mounts the part.
      # @return [Class<State>]
      def self.to_state_handle = @handle || raise(DefinitionError, "#{self}: declare part_of first")

      # Handler declarations (+evolve Part+, +reaction Part+) name the part's
      # compound state event, appended only by transitions that target the part
      # itself, not by those straight to one of its sub-states.
      # @return [Class<State>]
      def self.to_message_class = to_state_handle

      # A union of targets, as with states: +Part | Other+, +Part | nil+.
      # @return [Targets]
      def self.|(other) = Targets.new(members: [to_state_handle]) | other

      # @see State.history
      def self.history(deep: false) = to_state_handle.history(deep:)

      # Pattern matching, as its compound state's: @see State.===
      def self.===(other) = to_state_handle === other

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
        raise DefinitionError, "#{self}: State is already defined; a part's State is its compound state" if const_defined?(:State, false)

        if owner < Part
          raise DefinitionError, "#{self}: #{owner} has no part_of, so it's part of nothing yet" unless owner.owner

          @enclosing = owner
          owner = owner.owner
        end
        raise DefinitionError, "#{self}: declare partition_by on #{owner} before its parts" if owner.partition_keys.empty?

        @owner = owner
        @part_prefix = prefix&.to_s
        @handle = State.__generate(
          "#{owner.message_prefix}.runtime.#{part_prefix}",
          machine: self, state_name: part_prefix.split('.').last.to_sym, part: self
        )
        const_set(:State, @handle)
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
      # @param declared [Class<Sourced::Message>, #to_message_class]
      # @yield [context, message]
      def self.evolve(declared, &block)
        __owner!
        raise DefinitionError, "#{self}: evolve declared after #{owner} was validated" if @__locked
        raise DefinitionError, "#{self}: evolve #{declared} needs a block" unless block

        projection_class.evolve(declared, &block)
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
