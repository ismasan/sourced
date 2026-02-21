# frozen_string_literal: true

require 'sourced/types'

module Sourced
  module CCC
    # A query condition for reading messages from the store.
    # Matches on (message_type AND key_name AND key_value).
    # Multiple conditions are OR'd when passed to {Store#read}.
    QueryCondition = Data.define(:message_type, :key_name, :key_value)

    # Returned by {Store#read} and {Store#claim_next} for optimistic concurrency.
    # Pass to {Store#append} via +guard:+ to detect conflicting writes.
    ConsistencyGuard = Data.define(:conditions, :last_position)

    # Base message class for CCC's stream-less event sourcing.
    # Unlike {Sourced::Message}, CCC messages have no stream_id or seq
    # — they go into a flat, globally-ordered log.
    #
    # Supports +causation_id+ and +correlation_id+ for tracing causal chains
    # across messages, similar to {Sourced::Message}.
    #
    # Define message types via {.define}:
    #
    #   CourseCreated = CCC::Message.define('course.created') do
    #     attribute :course_name, String
    #   end
    #
    class Message < Types::Data
      EMPTY_ARRAY = [].freeze

      attribute :id, Types::AutoUUID
      attribute :type, Types::String.present
      attribute? :causation_id, Types::UUID::V4
      attribute? :correlation_id, Types::UUID::V4
      attribute :created_at, Types::Forms::Time.default { Time.now }
      attribute :metadata, Types::Hash.default(Plumb::BLANK_HASH)
      attribute :payload, Types::Static[nil]

      # Lookup table mapping type strings to message subclasses.
      # Separate from {Sourced::Message}'s registry.
      class Registry
        # @param message_class [Class] the root message class for this registry
        def initialize(message_class)
          @message_class = message_class
          @lookup = {}
        end

        # @return [Array<String>] registered type strings
        def keys = @lookup.keys

        # @return [Array<Class>] direct subclasses of the root message class
        def subclasses = message_class.subclasses

        # Register a message class under a type string.
        #
        # @param key [String] message type string
        # @param klass [Class] message subclass
        def []=(key, klass)
          @lookup[key] = klass
        end

        # Look up a message class by type string.
        # Searches this registry first, then recurses into subclass registries.
        #
        # @param key [String] message type string
        # @return [Class, nil]
        def [](key)
          klass = lookup[key]
          return klass if klass

          subclasses.each do |c|
            klass = c.registry[key]
            return klass if klass
          end
          nil
        end

        private

        attr_reader :lookup, :message_class
      end

      # @return [Registry] the message type registry for this class
      def self.registry
        @registry ||= Registry.new(self)
      end

      # Base class for typed message payloads.
      class Payload < Types::Data
        # @param key [Symbol] attribute name
        # @return [Object] attribute value
        def [](key) = attributes[key]

        # @see Hash#fetch
        def fetch(...) = to_h.fetch(...)
      end

      # Define a new message type. Registers it in the {.registry} and
      # optionally defines a typed payload.
      #
      # @param type_str [String] unique message type identifier (e.g. 'course.created')
      # @yield optional block to define payload attributes via +attribute+ DSL
      # @return [Class] the new message subclass
      #
      # @example
      #   UserJoined = CCC::Message.define('user.joined') do
      #     attribute :course_name, String
      #     attribute :user_id, String
      #   end
      def self.define(type_str, &payload_block)
        type_str.freeze unless type_str.frozen?

        registry[type_str] = Class.new(self) do
          def self.node_name = :data
          define_singleton_method(:type) { type_str }

          attribute :type, Types::Static[type_str]
          if block_given?
            payload_class = Class.new(Payload, &payload_block)
            const_set(:Payload, payload_class)
            attribute :payload, payload_class
            names = payload_class._schema.to_h.keys.map(&:to_sym).freeze
            define_singleton_method(:payload_attribute_names) { names }
          end
        end
      end

      # Instantiate the correct message subclass from a hash with a +:type+ key.
      #
      # @param attrs [Hash] must include +:type+ matching a registered type string
      # @return [Message] instance of the appropriate subclass
      # @raise [Sourced::UnknownMessageError] if the type string is not registered
      def self.from(attrs)
        klass = registry[attrs[:type]]
        raise Sourced::UnknownMessageError, "Unknown message type: #{attrs[:type]}" unless klass

        klass.new(attrs)
      end

      def initialize(attrs = {})
        attrs = attrs.merge(payload: {}) unless attrs[:payload]
        super(attrs)
      end

      # Set causation and correlation IDs on another message, establishing
      # a causal link from this message to +message+. Merges metadata.
      #
      # @param message [Message] the message to correlate
      # @return [Message] a copy of +message+ with causation/correlation set
      #
      # @example
      #   caused = source_event.correlate(SomeCommand.new(payload: { ... }))
      #   caused.causation_id  # => source_event.id
      #   caused.correlation_id # => source_event.correlation_id
      def correlate(message)
        attrs = {
          causation_id: id,
          correlation_id: correlation_id,
          metadata: metadata.merge(message.metadata || Plumb::BLANK_HASH)
        }
        message.with(attrs)
      end

      # Returns the declared payload attribute names for this message class.
      # Subclasses created via {.define} override this with a cached frozen array.
      #
      # @return [Array<Symbol>] attribute names (e.g. +[:course_name, :user_id]+)
      def self.payload_attribute_names = EMPTY_ARRAY

      # Build {QueryCondition}s for the intersection of this message's declared
      # attributes and the given key-value pairs. Attributes not declared on this
      # message class are silently ignored.
      #
      # @param attrs [Hash{Symbol => String}] partition attribute values
      # @return [Array<QueryCondition>]
      #
      # @example
      #   CourseCreated.to_conditions(course_name: 'Algebra', user_id: 'joe')
      #   # => [QueryCondition('course.created', 'course_name', 'Algebra')]
      #   # user_id ignored — CourseCreated doesn't declare it
      def self.to_conditions(**attrs)
        supported = payload_attribute_names
        attrs.filter_map do |key, value|
          next unless supported.include?(key)

          QueryCondition.new(
            message_type: type,
            key_name: key.to_s,
            key_value: value.to_s
          )
        end
      end

      # Auto-extract key-value pairs from all top-level payload attributes.
      # Used by {Store#append} to index messages for querying.
      #
      # @return [Array<Array(String, String)>] pairs of [name, value], skipping nils
      def extracted_keys
        return [] unless payload

        payload.to_h.filter_map { |k, v|
          [k.to_s, v.to_s] unless v.nil?
        }
      end

      private

      # Hook called by Plumb after schema parsing, when +:id+ has been resolved.
      # Defaults +causation_id+ and +correlation_id+ to the message's own +id+.
      def prepare_attributes(attrs)
        attrs[:correlation_id] = attrs[:id] unless attrs[:correlation_id]
        attrs[:causation_id] = attrs[:id] unless attrs[:causation_id]
        attrs
      end
    end
  end
end
