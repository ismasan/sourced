# frozen_string_literal: true

module Sourced
  # Partitioning by {Message#id}, one partition per message ({Selector#by_message_id}).
  # The reserved key name can't collide with a payload attribute.
  module IdPartition
    KEY = '__id'
  end

  # The messages to read: what {Store#read} reads and a {ConsistencyGuard} guards.
  #
  # A list of {Part}s, each with message types, partition keys and the values
  # bound to them. A part selects a message of its types that carries (in
  # payload or tags) at least one of its keys, every carried one with the bound
  # value; a part without keys selects every message of its types.
  #
  # An optional {Assertion} ({#is_empty}, {#includes}, ... combined with {#+},
  # {#|}, {#~}) states what must hold of what it selects. It covers its
  # selector as attached, so a later {#+} doesn't widen it. The store reads and
  # guards only the parts, and only bound selectors.
  #
  # @example
  #   s = Selector[CourseCreated, UserJoined].by(:course_id)
  #   s += Selector[UserRegistered].by(:user_id)
  #   store.read(s.bind(course_id: 'c1', user_id: 'u1'))
  #
  # @example An assertion
  #   cancelled = Selector[OrderCancelled, OrderReopened].by(:order_id).ends_with(OrderCancelled)
  #   placeable = (~cancelled).bind(order_id: 'o1')
  #   placeable.holds?(store.read(placeable).messages)
  class Selector < Data.define(:parts, :assertion)
    # An unbound key would otherwise select every value, widening the read.
    UnboundKeyError = Class.new(Sourced::Error)

    # Raised when defining an assertion that never holds, or always holds.
    AssertionError = Class.new(Sourced::DefinitionError)

    # Message types scoped by partition keys, and the values bound to them.
    # @!attribute types [Array<Class<Message>>] sorted by type string
    # @!attribute keys [Array<Symbol>] sorted
    # @!attribute values [Hash{Symbol => String}] bound values, by key
    Part = Data.define(:types, :keys, :values) do
      # @return [Array<String>]
      def type_names = types.map(&:type)

      # @example
      #   [CourseCreated, UserJoined].by(course_id: "c1", user_id: ?)
      def inspect
        names = types.map { |t| t.name || t.type }.join(', ')
        return "[#{names}]" if keys.empty?

        scope = keys.map { |k| values.key?(k) ? "#{k}: #{values[k].inspect}" : "#{k}: ?" }.join(', ')
        "[#{names}].by(#{scope})"
      end

      # @return [Boolean] whether every key has a value
      def bound? = keys.all? { |k| values.key?(k) }

      # Whether a message of {#types} carrying +pairs+ is in the part (see {Selector}).
      # @param pairs [Hash{String => String}] the message's {Message#extracted_keys}
      def selects_pairs?(pairs)
        return true if keys.empty?

        carried = keys.select { |k| pairs.key?(k.to_s) }
        !carried.empty? && carried.all? { |k| pairs[k.to_s] == values[k] }
      end

      def by(new_keys) = with(keys: (keys | new_keys).sort)

      def bind(new_values)
        bound = keys.each_with_object(values.dup) do |key, h|
          next unless new_values.key?(key)

          value = new_values[key]
          raise MissingPartitionKeyError, "can't bind #{key.inspect} to nil" if value.nil?

          value = value.to_s
          if h.key?(key) && h[key] != value
            raise ArgumentError, "#{key.inspect} is already bound to #{h[key].inspect}, not #{value.inspect}"
          end

          h[key] = value
        end
        with(values: bound.freeze)
      end
    end

    # A one-part selector, scoped by and bound to +values+ if given:
    # +Selector[E1, account_id: 1]+ is +Selector[E1].by(account_id: 1)+.
    #
    # @param types [Array<#to_message_class>]
    # @return [Selector] {NONE} for no types
    def self.[](*types, **values)
      return NONE if types.empty?

      selector = new(parts: [Part.new(types: types.map(&:to_message_class), keys: [], values: {})])
      values.empty? ? selector : selector.by(**values)
    end

    # Parts with the same keys and values merge, and are kept in a canonical
    # order, so equal selections compare equal however they were composed.
    def initialize(parts:, assertion: Assertion::TRUE)
      merged = parts.group_by { |p| [p.keys, p.values] }.map do |(keys, values), group|
        types = group.flat_map(&:types).uniq.sort_by(&:type).freeze
        Part.new(types:, keys: keys.freeze, values: values.freeze)
      end
      merged.sort_by! { |p| [p.keys, p.values.sort, p.type_names] }
      super(parts: merged.freeze, assertion:)
    end

    # Scope every part by +keys+ too, and by the keys of +values+, bound to them.
    #
    # @return [Selector]
    def by(*keys, **values)
      keys = (keys + values.keys).map(&:to_sym)
      scoped = with(parts: parts.map { |p| p.by(keys) }, assertion: assertion.map_scopes { |scope| scope.by(*keys) })
      values.empty? ? scoped : scoped.bind(**values)
    end

    # Scope only the parts without keys of their own by +keys+, leaving the
    # others and their assertions as declared: a reactor's partitioned read of
    # a source whose parts may have keys of their own.
    #
    # @return [Selector]
    def by_default(*keys)
      keys = keys.map(&:to_sym)
      with(
        parts: parts.map { |p| p.keys.empty? ? p.by(keys) : p },
        assertion: assertion.map_scopes { |scope| scope.by_default(*keys) }
      )
    end

    # Scope every part by message id (see {IdPartition}).
    # @return [Selector]
    def by_message_id = by(IdPartition::KEY)

    # @return [Boolean] whether it's scoped {#by_message_id}
    def id_partitioned? = keys == ID_KEYS

    # Bind values to the unbound keys of every part, ignoring keys a part lacks
    # (so a narrower selector binds from a wider partition). May be partial.
    #
    # @return [Selector]
    # @raise [MissingPartitionKeyError] for a nil value
    # @raise [ArgumentError] when rebinding a key to a different value
    def bind(**values)
      values = values.transform_keys(&:to_sym)
      with(parts: parts.map { |p| p.bind(values) }, assertion: assertion.map_scopes { |scope| scope.bind(**values) })
    end

    # What either selects; both assertions must hold.
    #
    # @raise [AssertionError] when the combined assertion never or always holds
    def +(other)
      combined = with(parts: parts + other.parts, assertion: Assertion.and(assertion, other.assertion))
      # An operand's assertion was checked when built. #by and #bind don't
      # check (bind is on the claim path), so one failing only once bound isn't caught.
      return combined if combined.assertion == assertion || combined.assertion == other.assertion

      combined.checked
    end

    # What either selects; either assertion must hold.
    #
    # @raise [ArgumentError] when either has no assertion: the result would always hold
    # @raise [AssertionError] when the combined assertion never or always holds
    def |(other)
      if assertion == Assertion::TRUE || other.assertion == Assertion::TRUE
        raise ArgumentError, "#{inspect} | #{other.inspect}: both sides need an assertion, or it always holds"
      end

      with(parts: parts + other.parts, assertion: Assertion.or(assertion, other.assertion)).checked
    end

    # The same selection, asserting the opposite.
    #
    # @raise [ArgumentError] without an assertion: the result would never hold
    def ~
      raise ArgumentError, "~#{inspect}: needs an assertion to negate" if assertion == Assertion::TRUE

      with(assertion: Assertion.not(assertion)).checked
    end

    # Holds when it selects nothing.
    def is_empty = attach(:is_empty, nil)

    # Holds when it selects some +type+ (one of its {#types}).
    def includes(type) = attach(:includes, type)

    # Holds when it selects no +type+.
    def excludes(type) = attach(:includes, type, negate: true)

    # Holds when the first message it selects is a +type+.
    def starts_with(type) = attach(:starts_with, type)

    # Holds when the last message it selects is a +type+.
    def ends_with(type) = attach(:ends_with, type)

    # Whether it selects +message+: the store's partition rule, in Ruby. It
    # matches id-indexed types by payload, which the store never does.
    #
    # @raise [UnboundKeyError] unless {#bound?}
    def selects?(message) = bound!.selected?(message.to_message)

    # @return [Array<Symbol>] its assertion's state before any message, one per leaf
    def start = assertion.leaves.map(&:initial).freeze

    # Its assertion's state after +messages+, in order, from +state+ ({#start}
    # or a previous {#advance}). Each leaf advances on what its scope selects.
    #
    # @return [Array<Symbol>]
    # @raise [UnboundKeyError] unless {#bound?}
    def advance(state, messages)
      bound!
      leaves = assertion.leaves
      return state if leaves.empty?

      messages.reduce(state) do |current, message|
        msg = message.to_message
        selected = {}.compare_by_identity # leaves attached together share a scope
        leaves.each_with_index.map do |leaf, i|
          in_scope = selected.fetch(leaf.scope) { selected[leaf.scope] = leaf.scope.selected?(msg) }
          in_scope ? leaf.step(current[i], msg.class) : current[i]
        end.freeze
      end
    end

    # @return [Boolean] whether its assertion holds in +state+
    def accepts?(state) = assertion.evaluate(assertion.leaves.zip(state).to_h)

    # @return [Boolean] whether its assertion holds after +messages+, in position order
    # @raise [UnboundKeyError] unless {#bound?}
    def holds?(messages) = accepts?(advance(start, messages))

    # @return [Automaton] its assertion's reachable states
    def automaton = Automaton.new(assertion, parts)

    # @return [String] a Mermaid diagram of its assertion's minimal automaton
    def to_mermaid = automaton.to_mermaid

    # A selector is a reactor's source (see {Evolve}), as an inline projector is.
    # @return [self]
    def selector = self

    # @return [Class<Projection>] a projector whose context is its types' messages
    def to_projector = Projection.for(self)

    # @return [Boolean] whether every key of every part has a value
    def bound? = parts.all?(&:bound?)

    # @return [Array<Symbol>] keys some part has no value for
    def unbound_keys = parts.flat_map { |p| p.keys - p.values.keys }.uniq

    # @raise [UnboundKeyError] unless {#bound?}
    def bound!
      return self if bound?

      raise UnboundKeyError, "can't read a selector with unbound keys #{unbound_keys.inspect}"
    end

    # @return [Boolean] whether it selects nothing
    def none? = parts.empty?

    # @return [Array<Class<Message>>] every part's types
    def types = parts.flat_map(&:types).uniq

    # @return [Array<String>]
    def type_names = types.map(&:type)

    # @return [Array<Symbol>] every part's keys
    def keys = parts.flat_map(&:keys).uniq

    # @example
    #   #<Sourced::Selector [CourseCreated].by(course_id: "c1") + [UserRegistered].by(user_id: ?)>
    #   #<Sourced::Selector [OrderCancelled, OrderReopened].by(order_id: ?) where ¬ends_with(OrderCancelled)>
    def inspect
      where = assertion == Assertion::TRUE ? '' : " where #{assertion.label}"
      return "#<#{self.class.name}::NONE#{where}>" if none?

      "#<#{self.class.name} #{parts.map(&:inspect).join(' + ')}#{where}>"
    end

    def pretty_print(q) = q.text(inspect)

    protected

    # {#selects?} without the binding check.
    def selected?(msg)
      klass = msg.class
      pairs = nil
      parts.any? do |part|
        next false unless part.types.include?(klass)

        pairs ||= msg.extracted_keys.to_h unless part.keys.empty?
        part.selects_pairs?(pairs || Plumb::BLANK_HASH)
      end
    end

    # @raise [AssertionError] when its assertion never or always holds
    def checked
      return self if assertion.leaves.empty?

      automaton = self.automaton
      raise AssertionError, "#{inspect} never holds" if automaton.never?
      raise AssertionError, "#{inspect} always holds" if automaton.always?

      self
    end

    private

    def attach(kind, type, negate: false)
      klass = type&.to_message_class
      if klass && !types.include?(klass)
        raise ArgumentError, "#{kind}(#{Assertion.type_label(klass)}): #{klass} isn't one of #{inspect}'s types, " \
                             'so it would never be read, nor guarded'
      end

      leaf = Assertion::Leaf.new(kind:, type: klass, scope: with(assertion: Assertion::TRUE))
      node = negate ? Assertion.not(leaf) : leaf
      with(assertion: Assertion.and(assertion, node)).checked
    end
  end
end

require 'sourced/selector/assertion'
require 'sourced/selector/automaton'
require 'sourced/selector/projection'

module Sourced
  class Selector
    # Selects nothing: the identity for {#+}.
    NONE = new(parts: [])

    # An id partition's keys (see {#id_partitioned?}).
    ID_KEYS = [IdPartition::KEY.to_sym].freeze
  end
end
