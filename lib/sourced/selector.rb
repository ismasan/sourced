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
  # Selectors are frozen values: every method returns a new one, so they compose
  # in any order. The store reads only bound selectors (see {#bound?}).
  #
  # @example
  #   s = Selector[CourseCreated, UserJoined].by(:course_id)
  #   s += Selector[UserRegistered].by(:user_id)
  #   store.read(s.bind(course_id: 'c1', user_id: 'u1'))
  class Selector < Data.define(:parts)
    # Raised when reading a selector whose keys aren't all bound: an unbound
    # key would otherwise select every value, silently widening the read.
    UnboundKeyError = Class.new(Sourced::Error)

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

    # A selector of one part, without keys.
    #
    # @param types [Array<#to_message_class>]
    # @return [Selector] {NONE} for no types
    def self.[](*types)
      return NONE if types.empty?

      new(parts: [Part.new(types: types.map(&:to_message_class), keys: [], values: {})])
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

    # Selects nothing: the identity for {#+}.
    NONE = new(parts: [])

    # Scope every part by +keys+, in addition to its own.
    #
    # @param keys [Array<Symbol, String>]
    # @return [Selector]
    def by(*keys)
      keys = keys.map(&:to_sym)
      with(parts: parts.map { |p| p.by(keys) })
    end

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
      with(parts: parts.map { |p| p.bind(values) })
    end

    # @param other [Selector]
    # @return [Selector] what either selects
    def +(other) = with(parts: parts + other.parts)

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

    # @return [Array<Symbol>] every part's keys
    def keys = parts.flat_map(&:keys).uniq

    # Each part as its types and scope, unbound keys as +?+.
    #
    # @example
    #   #<Sourced::Selector [CourseCreated].by(course_id: "c1") + [UserRegistered].by(user_id: ?)>
    def inspect
      return "#<#{self.class.name}::NONE>" if none?

      "#<#{self.class.name} #{parts.map(&:inspect).join(' + ')}>"
    end

    def pretty_print(q) = q.text(inspect)
  end
end
