# frozen_string_literal: true

module Sourced
  # A declarative description of the messages to read: what {Store#read} and
  # {Store#read_all} read, and what a {ConsistencyGuard} guards.
  #
  # A selector is a list of {Part}s, each with its own message types, partition
  # key names and the values bound to them. A part selects a message of one of
  # its types when every one of its keys that the message carries (in its
  # payload or tags) has the bound value, and the message carries at least one
  # of them. A part without keys selects every message of its types. A
  # selector selects what any of its parts selects.
  #
  # A selector can also assert what must hold of what it selects, in order:
  # {#is_empty}, {#includes}, {#excludes}, {#starts_with}, {#ends_with}, combined
  # with {#+} (and), {#|} (or) and {#~} (not). An assertion covers what its
  # selector selects at the point it's attached, so a later {#+} doesn't widen
  # it, while {#by} and {#bind} scope and bind it with the selector;
  # {#holds?} evaluates it over messages (see {Assertion}). The store reads
  # and guards only the parts.
  #
  # Selectors are frozen values: every method returns a new one, so they compose
  # in any order. The store reads only bound selectors (see {#bound?}).
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
    # Raised when reading a selector whose keys aren't all bound: an unbound
    # key would otherwise select every value, silently widening the read.
    UnboundKeyError = Class.new(Sourced::Error)

    # Raised when defining an assertion that never holds, or always holds.
    AssertionError = Class.new(Sourced::DefinitionError)

    # Message types scoped by partition keys, and the values bound to them.
    #
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

      # Whether a message of one of {#types} carrying these key pairs is in the
      # part: it carries at least one of its keys, each with the bound value.
      # Always true without keys.
      #
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

    # A selector of one part, without keys, or scoped by and bound to +values+
    # (see {#by}).
    #
    # @example
    #   Selector[E1, E2, account_id: 1] # Selector[E1, E2].by(account_id: 1)
    #
    # @param types [Array<#to_message_class>]
    # @param values [Hash{Symbol => Object}]
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

    # Scope every part by +keys+, in addition to its own, and by the keys of
    # +values+, bound to them.
    #
    # @example
    #   Selector[E1].by(account_id: 1) # Selector[E1].by(:account_id).bind(account_id: 1)
    #
    # @param keys [Array<Symbol, String>]
    # @param values [Hash{Symbol => Object}] see {#bind}
    # @return [Selector]
    def by(*keys, **values)
      keys = (keys + values.keys).map(&:to_sym)
      scoped = with(parts: parts.map { |p| p.by(keys) }, assertion: assertion.map_scopes { |scope| scope.by(*keys) })
      values.empty? ? scoped : scoped.bind(**values)
    end

    # Scope every part by message id: one partition per message, an
    # id-partitioned queue's claims (see {IdPartition}).
    #
    # @return [Selector]
    def by_message_id = by(IdPartition::KEY)

    # Bind values to the still-unbound keys of every part. Values for keys a part
    # doesn't have are ignored, so a narrower selector binds from a wider
    # partition. Binding can be partial, and happen in steps.
    #
    # @param values [Hash{Symbol, String => Object}] stringified
    # @return [Selector]
    # @raise [MissingPartitionKeyError] for a nil value
    # @raise [ArgumentError] when rebinding a key to a different value
    def bind(**values)
      values = values.transform_keys(&:to_sym)
      with(parts: parts.map { |p| p.bind(values) }, assertion: assertion.map_scopes { |scope| scope.bind(**values) })
    end

    # What either selects; both assertions must hold.
    #
    # @param other [Selector]
    # @return [Selector]
    # @raise [AssertionError] when the combined assertion never or always holds
    def +(other)
      combined = with(parts: parts + other.parts, assertion: Assertion.and(assertion, other.assertion))
      # Unchanged from an operand, which was checked when its assertion was
      # built (the built-ins, +, | and ~ go through #checked), so a check would
      # find the same. #by and #bind change scopes without checking: bind is on
      # the claim path, so an assertion that never (or always) holds only once
      # its keys are bound isn't caught.
      return combined if combined.assertion == assertion || combined.assertion == other.assertion

      combined.checked
    end

    # What either selects; either assertion must hold.
    #
    # @param other [Selector]
    # @return [Selector]
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
    # @return [Selector]
    # @raise [ArgumentError] without an assertion: the result would never hold
    def ~
      raise ArgumentError, "~#{inspect}: needs an assertion to negate" if assertion == Assertion::TRUE

      with(assertion: Assertion.not(assertion)).checked
    end

    # Holds when it selects nothing.
    # @return [Selector]
    def is_empty = attach(:is_empty, nil)

    # Holds when it selects some +type+.
    # @param type [#to_message_class] one of its types
    # @return [Selector]
    def includes(type) = attach(:includes, type)

    # Holds when it selects no +type+.
    # @param type [#to_message_class] one of its types
    # @return [Selector]
    def excludes(type) = attach(:includes, type, negate: true)

    # Holds when the first message it selects is a +type+.
    # @param type [#to_message_class] one of its types
    # @return [Selector]
    def starts_with(type) = attach(:starts_with, type)

    # Holds when the last message it selects is a +type+.
    # @param type [#to_message_class] one of its types
    # @return [Selector]
    def ends_with(type) = attach(:ends_with, type)

    # Whether the selector selects +message+: the store's partition rule, in
    # Ruby. Types indexed by message id (id-partitioned queues' types) are an
    # exception: the store indexes them under +__id+ only, so it never reads
    # them by a payload key, but this matches them by their payload.
    #
    # @param message [Message, #to_message]
    # @raise [UnboundKeyError] unless {#bound?}
    def selects?(message) = bound!.selected?(message.to_message)

    # The state of its assertion before any message: one state per leaf.
    #
    # @return [Array<Symbol>]
    def start = assertion.leaves.map(&:initial).freeze

    # The state of its assertion after +messages+, in order. Each leaf
    # advances on the messages its own selector selects.
    #
    # @param state [Array<Symbol>] from {#start} or a previous {#advance}
    # @param messages [Array<Message>]
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

    # @param state [Array<Symbol>]
    # @return [Boolean] whether its assertion holds in +state+
    def accepts?(state) = assertion.evaluate(assertion.leaves.zip(state).to_h)

    # @param messages [Array<Message>] in position order, ex. what {Store#read} returned
    # @return [Boolean] whether its assertion holds after +messages+
    # @raise [UnboundKeyError] unless {#bound?}
    def holds?(messages) = accepts?(advance(start, messages))

    # @return [Automaton] its assertion's reachable states (without an
    #   assertion, one accepting state over the types it selects)
    def automaton = Automaton.new(assertion, parts)

    # @return [String] a Mermaid +stateDiagram-v2+ of its assertion's minimal
    #   automaton, its states +valid+ (the assertion holds) or +invalid+. Without
    #   an assertion, one +valid+ state that every message it selects keeps.
    def to_mermaid = automaton.to_mermaid

    # @return [Boolean] whether every key of every part has a value
    def bound? = parts.all?(&:bound?)

    # @return [Array<Symbol>] keys some part has no value for
    def unbound_keys = parts.flat_map { |p| p.keys - p.values.keys }.uniq

    # @return [self]
    # @raise [UnboundKeyError] unless {#bound?}
    def bound!
      return self if bound?

      raise UnboundKeyError, "can't read a selector with unbound keys #{unbound_keys.inspect}"
    end

    # @return [Boolean] whether it selects nothing
    def none? = parts.empty?

    # @return [Array<Class<Message>>] every part's types
    def types = parts.flat_map(&:types).uniq

    # @return [Array<String>] every part's type strings
    def type_names = types.map(&:type)

    # @return [Array<Symbol>] every part's keys
    def keys = parts.flat_map(&:keys).uniq

    # Each part as its types and scope, unbound keys as +?+, and its assertion.
    #
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

    # @return [self]
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

module Sourced
  class Selector
    # Selects nothing: the identity for {#+}.
    NONE = new(parts: [])
  end
end
