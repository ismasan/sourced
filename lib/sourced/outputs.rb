# frozen_string_literal: true

module Sourced
  # What a handler may emit (a {Decider.command}'s, a {React.reaction}'s, a
  # machine's +entry+, +exit+ and +after+ actions'): the message classes it
  # declares, and whether it may emit none (+| nil+).
  Outputs = Data.define(:messages, :optional) do
    # One +Input => outputs+ pair, as +command+, +reaction+ and +after+ take
    # it: the input as given (each macro reads its own), and its outputs.
    #
    #   input, outputs = Outputs.pair(declaration, "#{self}.command", example: 'Command => Event')
    #
    # @param declaration [Object] what the macro was given
    # @param label [String] the macro, for errors
    # @param example [String] a pair it takes, for errors
    # @param error [Class<Exception>] what to raise
    # @return [Array(Object, Outputs)]
    # @raise [error] for anything but one pair, or outputs {.declare} rejects
    def self.pair(declaration, label, example:, error: ArgumentError)
      unless Hash === declaration && declaration.size == 1
        raise error, "#{label} takes one pair, ex. #{example}; got #{declaration.inspect}"
      end

      input, declared = declaration.first
      [input, declare(declared, "#{label} #{input.inspect}", error:)]
    end

    # {.parse}, raising +error+ with +label+ for what it rejects.
    # @param declared [Class, Plumb::Or, nil]
    # @param label [String] the declaring macro, for errors
    # @param error [Class<Exception>]
    # @return [Outputs]
    def self.declare(declared, label, error: ArgumentError)
      parse(declared)
    rescue ArgumentError => e
      raise error, "#{label}: #{e.message}"
    end

    # @param declared [Class, Plumb::Or, nil] a message class, a union of
    #   them (+A | B+), with +nil+ (+A | nil+) for "may emit none"
    # @return [Outputs]
    # @raise [ArgumentError] for anything that isn't a message class or nil
    def self.parse(declared)
      members = flatten(declared)
      new(messages: members.compact.uniq.freeze, optional: members.include?(nil))
    end

    # The message classes +declared+ stands for, +nil+ for a +nil+ member: a
    # message class (or anything answering +to_message_class+), a +Plumb::Or+
    # of them, or a union answering +to_message_classes+ (ex. machine states,
    # {Machine::Targets}).
    # @return [Array<Class, nil>]
    # @raise [ArgumentError] for anything else
    def self.flatten(declared)
      case declared
      in nil then [nil]
      in Plumb::ValueClass if declared.children == [nil] then [nil]
      in Plumb::Or then declared.children.flat_map { flatten(it) }
      else
        # Checked when a handler is declared.
        return declared.to_message_classes if declared.respond_to?(:to_message_classes)
        raise ArgumentError, "#{declared.inspect} isn't a message class" unless declared.respond_to?(:to_message_class)

        [declared.to_message_class]
      end
    end

    # An action without a block dispatches each output, so it needs some, and
    # no choice between dispatching them and none.
    # @param block [Proc, nil] the action's block
    # @param label [String] the action, for errors
    # @param error [Class<Exception>]
    # @return [self]
    def check_blockless!(block, label, error: ArgumentError)
      return self if block || (messages.any? && !optional)

      raise error, "#{label}: #{self} needs a block to choose what to dispatch"
    end

    # @return [Boolean] whether a handler may emit +message_class+
    def include?(message_class) = messages.include?(message_class)

    def to_s = [*messages, *(optional ? ['nil'] : [])].join(' | ')
  end
end
