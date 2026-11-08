# frozen_string_literal: true

module Sourced
  # What a handler may emit (a {Decider.command}'s, a {React.reaction}'s):
  # the message classes it declares, and whether it may emit none (+| nil+).
  Outputs = Data.define(:messages, :optional) do
    # @param declared [Class, Plumb::Or, nil] a message class, a union of
    #   them (+A | B+), with +nil+ (+A | nil+) for "may emit none"
    # @return [Outputs]
    # @raise [ArgumentError] for anything that isn't a message class or nil
    def self.parse(declared)
      members = flatten(declared)
      new(messages: members.compact.uniq.freeze, optional: members.include?(nil))
    end

    def self.flatten(declared)
      case declared
      in nil then [nil]
      in Plumb::ValueClass if declared.children == [nil] then [nil]
      in Plumb::Or then declared.children.flat_map { flatten(it) }
      else
        unless declared.respond_to?(:to_message_class)
          raise ArgumentError, "#{declared.inspect} isn't a message class"
        end

        [declared.to_message_class]
      end
    end

    # @return [Boolean] whether a handler may emit +message_class+
    def include?(message_class) = messages.include?(message_class)

    def to_s = [*messages, *(optional ? ['nil'] : [])].join(' | ')
  end
end
