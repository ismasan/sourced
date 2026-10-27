# frozen_string_literal: true

module Sourced
  class Selector
    # What must hold of the messages a {Selector} selects, as a tree of built-in
    # assertions ({Leaf}s) combined with {And}, {Or} and {Not}.
    #
    # Every built-in is a small deterministic finite automaton over message
    # types, so a combination is their product: its state is one state per
    # leaf, advanced by the messages each leaf's own selector (its +scope+, the
    # selector it was attached to) selects. That state is all the history it
    # needs to decide, so it can be stored and advanced with new messages only
    # (see {Selector#start}, {Selector#advance}, {Selector#accepts?}).
    #
    # Nodes are frozen values. {.and} / {.or} flatten and order their operands,
    # so equal assertions compare equal however they were combined.
    module Assertion
      # Holds for any history: a selector without an assertion.
      True = Data.define do
        def leaves = Plumb::BLANK_ARRAY
        def map_scopes = self
        def evaluate(_states) = true
        def label = 'true'
        def sort_key = label
      end
      TRUE = True.new

      # A built-in assertion over the messages +scope+ selects.
      #
      # @!attribute kind [Symbol] +:is_empty+, +:includes+, +:starts_with+ or +:ends_with+
      # @!attribute type [Class<Message>, nil] the message class it's about (nil for +:is_empty+)
      # @!attribute scope [Selector] the selector it was attached to, without an assertion
      Leaf = Data.define(:kind, :type, :scope) do
        def leaves = [self]
        def map_scopes(&) = with(scope: yield(scope))
        def evaluate(states) = accept?(states.fetch(self))

        # @return [Symbol]
        def initial
          case kind
          in :is_empty then :empty
          in :includes then :none
          in :starts_with then :start
          in :ends_with then :other
          end
        end

        # The state after a message of +klass+ that {#scope} selects.
        #
        # @param state [Symbol]
        # @param klass [Class<Message>]
        # @return [Symbol]
        def step(state, klass)
          case kind
          in :is_empty then :some
          in :includes then klass == type ? :seen : state
          in :starts_with then state == :start ? (klass == type ? :yes : :no) : state
          in :ends_with then klass == type ? :last : :other
          end
        end

        # @param state [Symbol]
        def accept?(state)
          case kind
          in :is_empty then state == :empty
          in :includes then state == :seen
          in :starts_with then state == :yes
          in :ends_with then state == :last
          end
        end

        def label = type ? "#{kind}(#{Assertion.type_label(type)})" : kind.to_s
        def sort_key = "#{label} #{scope.inspect}"
      end

      # Holds when +operand+ doesn't.
      Not = Data.define(:operand) do
        def leaves = operand.leaves
        def map_scopes(&) = with(operand: operand.map_scopes(&))
        def evaluate(states) = !operand.evaluate(states)
        def label = "¬#{operand.is_a?(Leaf) || operand.is_a?(Not) ? operand.label : "(#{operand.label})"}"
        def sort_key = "¬#{operand.sort_key}"
      end

      # Holds when all +operands+ do.
      And = Data.define(:operands) do
        def leaves = operands.flat_map(&:leaves).uniq
        def map_scopes(&) = Assertion.and(*operands.map { |o| o.map_scopes(&) })
        def evaluate(states) = operands.all? { |o| o.evaluate(states) }
        def label = operands.map { |o| o.is_a?(Or) ? "(#{o.label})" : o.label }.join(' ∧ ')
        def sort_key = "∧(#{operands.map(&:sort_key).join(', ')})"
      end

      # Holds when any of +operands+ does.
      Or = Data.define(:operands) do
        def leaves = operands.flat_map(&:leaves).uniq
        def map_scopes(&) = Assertion.or(*operands.map { |o| o.map_scopes(&) })
        def evaluate(states) = operands.any? { |o| o.evaluate(states) }
        def label = operands.map { |o| o.is_a?(And) ? "(#{o.label})" : o.label }.join(' ∨ ')
        def sort_key = "∨(#{operands.map(&:sort_key).join(', ')})"
      end

      module_function

      # @return [Assertion] all of +nodes+, flattened, deduplicated and ordered;
      #   {TRUE} for none
      def and(*nodes)
        operands = nodes.flat_map { |n| n.is_a?(And) ? n.operands : [n] }.reject { |n| n == TRUE }
        combine(And, operands) { TRUE }
      end

      # @return [Assertion] any of +nodes+, flattened, deduplicated and ordered
      def or(*nodes)
        operands = nodes.flat_map { |n| n.is_a?(Or) ? n.operands : [n] }
        return TRUE if operands.include?(TRUE)

        combine(Or, operands) { raise ArgumentError, 'an Or needs operands' }
      end

      # @return [Assertion] the negation of +node+ (double negation cancels)
      def not(node) = node.is_a?(Not) ? node.operand : Not.new(operand: node)

      def combine(klass, operands)
        operands = operands.uniq.sort_by(&:sort_key)
        return yield if operands.empty?
        return operands.first if operands.size == 1

        klass.new(operands: operands.freeze)
      end

      # A message class's name for labels: its constant's last segment, or its type.
      def type_label(klass) = klass.name ? klass.name.split('::').last : klass.type
    end
  end
end
