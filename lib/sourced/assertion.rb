# frozen_string_literal: true

module Sourced
  # A predicate over a message history, by message type: built-in {Leaf}s
  # ({.is_empty}, {.includes}, {.excludes}, {.starts_with}, {.ends_with})
  # combined with +, | and ~. For checks inside handlers, over the history
  # their partition read (a selector source's context is its messages).
  #
  # {.and} / {.or} flatten and order their operands, so equal assertions compare equal.
  #
  # @example
  #   Cancelled = Sourced::Assertion.ends_with(OrderCancelled)
  #   Shippable = Sourced::Assertion.includes(PaymentReceived) + ~Cancelled
  #
  #   command Ship => Shipped | ShipmentRejected do |history, cmd|
  #     next event(ShipmentRejected, order_id: cmd.payload.order_id) unless Shippable === history
  #     ...
  #   end
  module Assertion
    # What every assertion answers.
    module Node
      # @return [Assertion] holds when both do
      def +(other) = Assertion.and(self, other)

      # @return [Assertion] holds when either does
      def |(other) = Assertion.or(self, other)

      # @return [Assertion] holds when this doesn't
      def ~ = Assertion.not(self)

      # @param messages [Array<Sourced::Message>] in log order
      # @return [Boolean]
      # @raise [ArgumentError] for anything but an Array, ex. an inline projector's context
      def holds?(messages)
        raise ArgumentError, "#{inspect} takes an Array of messages, got #{messages.class}" unless messages.is_a?(Array)

        evaluate(messages.map { |msg| msg.to_message.class })
      end

      alias === holds?

      def inspect = "#<Sourced::Assertion #{label}>"
    end

    # A built-in assertion.
    #
    # @!attribute kind [Symbol] +:is_empty+, +:includes+, +:starts_with+ or +:ends_with+
    # @!attribute type [Class<Message>, nil] the message class it's about (nil for +:is_empty+)
    Leaf = Data.define(:kind, :type) do
      include Node

      def evaluate(types)
        case kind
        in :is_empty then types.empty?
        in :includes then types.include?(type)
        in :starts_with then types.first == type
        in :ends_with then types.last == type
        end
      end

      def label = type ? "#{kind}(#{Assertion.type_label(type)})" : kind.to_s
      def sort_key = label
    end

    # Holds when +operand+ doesn't.
    Not = Data.define(:operand) do
      include Node

      def evaluate(types) = !operand.evaluate(types)
      def label = "¬#{operand.is_a?(Leaf) || operand.is_a?(Not) ? operand.label : "(#{operand.label})"}"
      def sort_key = "¬#{operand.sort_key}"
    end

    # Holds when all +operands+ do.
    And = Data.define(:operands) do
      include Node

      def evaluate(types) = operands.all? { |o| o.evaluate(types) }
      def label = operands.map { |o| o.is_a?(Or) ? "(#{o.label})" : o.label }.join(' ∧ ')
      def sort_key = "∧(#{operands.map(&:sort_key).join(', ')})"
    end

    # Holds when any of +operands+ does.
    Or = Data.define(:operands) do
      include Node

      def evaluate(types) = operands.any? { |o| o.evaluate(types) }
      def label = operands.map { |o| o.is_a?(And) ? "(#{o.label})" : o.label }.join(' ∨ ')
      def sort_key = "∨(#{operands.map(&:sort_key).join(', ')})"
    end

    module_function

    # Holds for a history with no messages.
    def is_empty = Leaf.new(kind: :is_empty, type: nil)

    # Holds when the history has a +type+.
    def includes(type) = Leaf.new(kind: :includes, type: type.to_message_class)

    # Holds when the history has no +type+.
    def excludes(type) = self.not(includes(type))

    # Holds when the first message is a +type+.
    def starts_with(type) = Leaf.new(kind: :starts_with, type: type.to_message_class)

    # Holds when the last message is a +type+, ex. an order cancelled and not
    # reopened since.
    def ends_with(type) = Leaf.new(kind: :ends_with, type: type.to_message_class)

    # @return [Assertion] all of +nodes+, flattened, deduplicated and ordered
    def and(*nodes) = combine(And, nodes.flat_map { |n| n.is_a?(And) ? n.operands : [n] })

    # @return [Assertion] any of +nodes+, flattened, deduplicated and ordered
    def or(*nodes) = combine(Or, nodes.flat_map { |n| n.is_a?(Or) ? n.operands : [n] })

    # @return [Assertion] the negation of +node+ (double negation cancels)
    def not(node) = node.is_a?(Not) ? node.operand : Not.new(operand: node)

    def combine(klass, operands)
      operands = operands.uniq.sort_by(&:sort_key)
      operands.size == 1 ? operands.first : klass.new(operands: operands.freeze)
    end

    # A message class's name for labels: its constant's last segment, or its type.
    def type_label(klass) = klass.name ? klass.name.split('::').last : klass.type
  end
end
