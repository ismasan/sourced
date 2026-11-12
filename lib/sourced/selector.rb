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
  # value; a part without keys selects every message of its types. The store
  # reads only bound selectors.
  #
  # @example
  #   s = Selector[CourseCreated, UserJoined].by(:course_id)
  #   s += Selector[UserRegistered].by(:user_id)
  #   store.read(s.bind(course_id: 'c1', user_id: 'u1'))
  class Selector < Data.define(:parts)
    # An unbound key would otherwise select every value, widening the read.
    UnboundKeyError = Class.new(Sourced::Error)

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
    def initialize(parts:)
      merged = parts.group_by { |p| [p.keys, p.values] }.map do |(keys, values), group|
        types = group.flat_map(&:types).uniq.sort_by(&:type).freeze
        Part.new(types:, keys: keys.freeze, values: values.freeze)
      end
      merged.sort_by! { |p| [p.keys, p.values.sort, p.type_names] }
      super(parts: merged.freeze)
    end

    # Scope every part by +keys+ too, and by the keys of +values+, bound to them.
    #
    # @return [Selector]
    def by(*keys, **values)
      keys = (keys + values.keys).map(&:to_sym)
      scoped = with(parts: parts.map { |p| p.by(keys) })
      values.empty? ? scoped : scoped.bind(**values)
    end

    # Scope only the parts without keys of their own by +keys+, leaving the
    # others as declared: a reactor's partitioned read of a source whose parts
    # may have keys of their own.
    #
    # @return [Selector]
    def by_default(*keys)
      keys = keys.map(&:to_sym)
      with(parts: parts.map { |p| p.keys.empty? ? p.by(keys) : p })
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
      with(parts: parts.map { |p| p.bind(values) })
    end

    # @return [Selector] what either selects
    def +(other) = with(parts: parts + other.parts)

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
    def inspect
      return "#<#{self.class.name}::NONE>" if none?

      "#<#{self.class.name} #{parts.map(&:inspect).join(' + ')}>"
    end

    def pretty_print(q) = q.text(inspect)
  end
end

require 'sourced/selector/projection'

module Sourced
  class Selector
    # Selects nothing: the identity for {#+}.
    NONE = new(parts: [])

    # An id partition's keys (see {#id_partitioned?}).
    ID_KEYS = [IdPartition::KEY.to_sym].freeze
  end
end
