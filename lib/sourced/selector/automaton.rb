# frozen_string_literal: true

module Sourced
  class Selector
    # The reachable states of an {Assertion}'s product automaton, for checking
    # assertions when they're defined and drawing them.
    #
    # A letter is a message class and the part scopes that select it. With
    # every key bound, the alphabet is the combinations some message can
    # produce (one +order_id+ can't be both "x" and "y"); with unbound keys it's
    # every combination, an over-approximation. A superset of the real alphabet
    # reaches a superset of the real states, so {#never?} and {#always?} stay
    # sound. Don't tighten it without keeping that.
    class Automaton
      # A message class, selected by the parts with these signatures (keys and values).
      Letter = Data.define(:type, :signatures) do
        # @param scoped [Boolean] whether to name the scopes that select it
        def label(scoped: false)
          name = Assertion.type_label(type)
          return name unless scoped

          scopes = signatures.map do |keys, values|
            keys.empty? ? 'any' : keys.map { |k| values.key?(k) ? "#{k}: #{values[k].inspect}" : k.to_s }.join(', ')
          end
          "#{name} (#{scopes.join(' | ')})"
        end
      end

      # @return [Array<Assertion::Leaf>]
      attr_reader :leaves
      # @return [Array<Letter>]
      attr_reader :alphabet
      # @return [Array<Array<Symbol>>] reachable states, in breadth-first order from {#initial}
      attr_reader :states
      # @return [Array<Symbol>] one state per leaf
      attr_reader :initial
      # @return [Hash{Array<Symbol> => Hash{Letter => Array<Symbol>}}]
      attr_reader :transitions
      # @return [Set<Array<Symbol>>]
      attr_reader :accepting

      # @param parts [Array<Selector::Part>] the alphabet's parts when the assertion has no leaves
      def initialize(assertion, parts = Plumb::BLANK_ARRAY)
        @assertion = assertion
        @leaves = assertion.leaves
        @parts = @leaves.empty? ? parts : @leaves.flat_map { |leaf| leaf.scope.parts }
        @alphabet = build_alphabet
        @initial = leaves.map(&:initial).freeze
        explore
      end

      # @return [Boolean] whether no history makes the assertion hold
      def never? = accepting.empty?

      # @return [Boolean] whether every history makes the assertion hold
      def always? = accepting.size == states.size

      # @param state [Array<Symbol>]
      def accept?(state) = accepting.include?(state)

      # States merged when they accept the same futures (Moore's algorithm).
      # @return [Array<Array<Array<Symbol>>>] blocks of equivalent states; the first holds {#initial}
      def minimal_blocks
        block_of = states.to_h { |s| [s, accept?(s) ? 1 : 0] }
        loop do
          signatures = states.to_h { |s| [s, [block_of[s], *alphabet.map { |l| block_of[transitions[s][l]] }]] }
          ids = {}
          refined = states.to_h { |s| [s, ids[signatures[s]] ||= ids.size] }
          break if refined.values.uniq.size == block_of.values.uniq.size

          block_of = refined
        end
        states.group_by { |s| block_of[s] }.values
      end

      # @return [String] a Mermaid +stateDiagram-v2+ of the minimal automaton,
      #   its states +valid+ or +invalid+ (whether the assertion holds there)
      def to_mermaid
        blocks = minimal_blocks
        id_of = {}
        blocks.each_with_index { |block, i| block.each { |s| id_of[s] = "s#{i}" } }

        # Name scopes only for types more than one scope selects.
        scoped = alphabet.group_by(&:type).select { |_, letters| letters.size > 1 }.keys
        lines = ['stateDiagram-v2', '    classDef accepting font-weight:bold,stroke-width:3px']
        blocks.each_with_index do |block, i|
          lines << %(    state "#{accept?(block.first) ? 'valid' : 'invalid'}" as s#{i})
        end
        lines << '    [*] --> s0'
        blocks.each do |block|
          from = block.first
          edges = alphabet.group_by { |letter| id_of[transitions[from][letter]] }
          edges.each do |to, letters|
            labels = letters.map { |l| l.label(scoped: scoped.include?(l.type)) }
            lines << "    #{id_of[from]} --> #{to}: #{labels.join(', ')}"
          end
        end
        accepting_ids = blocks.each_index.select { |i| accept?(blocks[i].first) }
        lines << "    class #{accepting_ids.map { |i| "s#{i}" }.join(',')} accepting" if accepting_ids.any?
        lines.join("\n")
      end

      private

      def build_alphabet
        signatures = Hash.new { |h, k| h[k] = [] }
        @parts.each do |part|
          part.types.each { |type| signatures[type] |= [[part.keys, part.values]] }
        end
        signatures.flat_map do |type, sigs|
          (1..sigs.size).flat_map do |n|
            sigs.combination(n).filter_map do |subset|
              Letter.new(type:, signatures: subset) if realisable?(sigs, subset)
            end
          end
        end
      end

      # Whether some message's key pairs are selected by exactly +subset+ of
      # +sigs+. Assumed when a key is unbound (see the class comment).
      def realisable?(sigs, subset)
        return true if sigs.any? { |keys, values| keys.any? { |k| !values.key?(k) } }

        keys = sigs.flat_map(&:first).uniq
        # Each key absent, carrying a value no part binds, or one a part binds.
        domains = keys.map { |k| [nil, :other, *sigs.filter_map { |ks, vs| vs[k] if ks.include?(k) }.uniq] }
        assignments = domains.reduce([[]]) { |acc, domain| acc.flat_map { |vals| domain.map { |v| vals + [v] } } }
        assignments.any? do |vals|
          pairs = keys.zip(vals).reject { |_, v| v.nil? }.to_h
          selecting = sigs.select do |ks, vs|
            carried = ks.select { |k| pairs.key?(k) }
            ks.empty? || (carried.any? && carried.all? { |k| pairs[k] == vs[k] })
          end
          selecting.to_set == subset.to_set
        end
      end

      def explore
        @states = [initial]
        @transitions = {}
        seen = Set[initial]
        queue = [initial]
        until queue.empty?
          state = queue.shift
          @transitions[state] = alphabet.to_h do |letter|
            succ = step(state, letter)
            if seen.add?(succ)
              @states << succ
              queue << succ
            end
            [letter, succ]
          end
        end
        @accepting = states.select { |s| @assertion.evaluate(leaves.zip(s).to_h) }.to_set
      end

      def step(state, letter)
        leaves.each_with_index.map do |leaf, i|
          selects = leaf.scope.parts.any? do |part|
            part.types.include?(letter.type) && letter.signatures.include?([part.keys, part.values])
          end
          selects ? leaf.step(state[i], letter.type) : state[i]
        end.freeze
      end
    end
  end
end
