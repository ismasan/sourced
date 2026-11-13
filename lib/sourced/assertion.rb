# frozen_string_literal: true

module Sourced
  # A predicate over a message history, by message type: built-in {Leaf}s
  # ({.is_empty}, {.includes}, {.excludes}, {.starts_with}, {.ends_with}, or
  # {.[]} for short) combined with +, -, | and ~. For checks inside handlers,
  # over the history their partition read (a selector source's context is its
  # messages).
  #
  # {.and} / {.or} flatten and order their operands, so equal assertions compare equal.
  #
  # @example
  #   Cancelled = Sourced::Assertion[..OrderCancelled]
  #   Shippable = Sourced::Assertion[PaymentReceived] - Cancelled
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

      # @return [Assertion] holds when this does and +other+ doesn't
      def -(other) = self + ~other

      # Chained builders: this and the built-in, ex.
      # <tt>Assertion.includes(A).includes(B).ends_with(C)</tt>.

      # @return [Assertion] holds when this does and the history has a +type+
      def includes(type) = self + Assertion.includes(type)

      # @return [Assertion] holds when this does and the history has no +type+
      def excludes(type) = self + Assertion.excludes(type)

      # @return [Assertion] holds when this does and the first message is a +type+
      def starts_with(type) = self + Assertion.starts_with(type)

      # @return [Assertion] holds when this does and the last message is a +type+
      def ends_with(type) = self + Assertion.ends_with(type)

      # @param messages [Array<Sourced::Message>] in log order
      # @return [Boolean]
      # @raise [ArgumentError] for anything but an Array, ex. an inline projector's context
      def holds?(messages)
        raise ArgumentError, "#{inspect} takes an Array of messages, got #{messages.class}" unless messages.is_a?(Array)

        evaluate(messages.map { |msg| msg.to_message.class })
      end

      alias === holds?

      # @return [String] the assertion as it's built, ex.
      #   <tt>Assertion[Opened.., ..Closed] - Assertion[Paid]</tt>
      def inspect = to_s

      # For pp and IRB, which otherwise print Data's own form.
      def pretty_print(pp) = pp.text(inspect)

      # Whether {#to_s} needs no parentheses as an operand of +, - or ~.
      def atomic? = true
    end

    # A built-in assertion: one item of {Assertion.[]}.
    module Leaf
      include Node

      def to_s = "Assertion[#{item}]"
    end

    # Holds for a history with no messages.
    IsEmpty = Data.define do
      include Leaf

      def evaluate(types) = types.empty?
      def item = ''
      def sort_key = '0'
    end

    # A built-in assertion about one message type. Sort keys order a
    # bracket's items as written: starts with, includes, ends with.
    #
    # @!attribute type [Class<Message>]
    TypeLeaf = Data.define(:type) do
      include Leaf

      def type_label = Assertion.type_label(type)
    end

    # Holds when the first message is a +type+.
    class StartsWith < TypeLeaf
      def evaluate(types) = types.first == type
      def item = "#{type_label}.."
      def sort_key = "1 #{type.type}"
    end

    # Holds when the history has a +type+.
    class Includes < TypeLeaf
      def evaluate(types) = types.include?(type)
      def item = type_label
      def sort_key = "2 #{type.type}"
    end

    # Holds when the last message is a +type+.
    class EndsWith < TypeLeaf
      def evaluate(types) = types.last == type
      def item = "..#{type_label}"
      def sort_key = "3 #{type.type}"
    end

    # Holds when +operand+ doesn't.
    Not = Data.define(:operand) do
      include Node

      def evaluate(types) = !operand.evaluate(types)
      def to_s = "~#{Assertion.group(operand)}"
      def sort_key = "¬#{operand.sort_key}"
    end

    # Holds when all +operands+ do. Written as one bracket of its leaves, the
    # rest added, and negations subtracted.
    And = Data.define(:operands) do
      include Node

      def evaluate(types) = operands.all? { |o| o.evaluate(types) }
      def sort_key = "∧(#{operands.map(&:sort_key).join(', ')})"
      def atomic? = operands.all?(TypeLeaf)

      def to_s
        nots, added = operands.partition { |o| o.is_a?(Not) }
        head = added.empty? ? nots.shift.to_s : sum(added)
        [head, *nots.map { |n| Assertion.group(n.operand) }].join(' - ')
      end

      private

      def sum(added)
        items, others = added.partition { |o| o.is_a?(TypeLeaf) }
        terms = others.map { |o| Assertion.group(o) }
        terms.unshift("Assertion[#{items.map(&:item).join(', ')}]") if items.any?
        terms.join(' + ')
      end
    end

    # Holds when any of +operands+ does.
    Or = Data.define(:operands) do
      include Node

      def evaluate(types) = operands.any? { |o| o.evaluate(types) }
      def sort_key = "∨(#{operands.map(&:sort_key).join(', ')})"
      def to_s = operands.join(' | ')
      def atomic? = false
    end

    module_function

    # Built-ins for short, all of which must hold: a message class for
    # {.includes}, <tt>A..</tt> for {.starts_with}, <tt>..A</tt> for {.ends_with},
    # none for {.is_empty}.
    #
    # @example
    #   Assertion[Paid, Shipped]       # includes both
    #   Assertion[Opened.., ..Closed]  # starts with Opened and ends with Closed
    # @raise [ArgumentError] for a range with both ends or none, or an exclusive one
    def [](*items)
      return is_empty if items.empty?

      self.and(*items.map { |item| item.is_a?(Range) ? range_leaf(item) : includes(item) })
    end

    # Holds for a history with no messages.
    def is_empty = IsEmpty.new

    # Holds when the history has a +type+.
    def includes(type) = Includes.new(type: type.to_message_class)

    # Holds when the history has no +type+.
    def excludes(type) = self.not(includes(type))

    # Holds when the first message is a +type+.
    def starts_with(type) = StartsWith.new(type: type.to_message_class)

    # Holds when the last message is a +type+, ex. an order cancelled and not
    # reopened since.
    def ends_with(type) = EndsWith.new(type: type.to_message_class)

    # @return [Assertion] all of +nodes+, flattened, deduplicated and ordered
    def and(*nodes) = combine(And, nodes.flat_map { |n| n.is_a?(And) ? n.operands : [n] })

    # @return [Assertion] any of +nodes+, flattened, deduplicated and ordered
    def or(*nodes) = combine(Or, nodes.flat_map { |n| n.is_a?(Or) ? n.operands : [n] })

    # @return [Assertion] the negation of +node+ (double negation cancels)
    def not(node) = node.is_a?(Not) ? node.operand : Not.new(operand: node)

    def range_leaf(range)
      if range.exclude_end? || range.begin.nil? == range.end.nil?
        raise ArgumentError, "Assertion[] takes A.. (starts with) or ..A (ends with), got #{range.inspect}"
      end

      range.begin ? starts_with(range.begin) : ends_with(range.end)
    end

    def combine(klass, operands)
      operands = operands.uniq.sort_by(&:sort_key)
      operands.size == 1 ? operands.first : klass.new(operands: operands.freeze)
    end

    # +node+ as an operand of +, - or ~: in parentheses unless {Node#atomic?}.
    def group(node) = node.atomic? ? node.to_s : "(#{node})"

    # A message class's name for labels: its constant's last segment, or its type.
    def type_label(klass) = klass.name ? klass.name.split('::').last : klass.type
  end
end
