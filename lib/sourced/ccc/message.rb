# frozen_string_literal: true

require 'sourced/types'

module Sourced
  module CCC
    QueryCondition = Data.define(:message_type, :key_name, :key_value)
    ConsistencyGuard = Data.define(:conditions, :last_position)

    class Message < Types::Data
      EMPTY_ARRAY = [].freeze

      attribute :id, Types::AutoUUID
      attribute :type, Types::String.present
      attribute :created_at, Types::Forms::Time.default { Time.now }
      attribute :metadata, Types::Hash.default(Plumb::BLANK_HASH)
      attribute :payload, Types::Static[nil]

      class Registry
        def initialize(message_class)
          @message_class = message_class
          @lookup = {}
        end

        def keys = @lookup.keys
        def subclasses = message_class.subclasses

        def []=(key, klass)
          @lookup[key] = klass
        end

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

      def self.registry
        @registry ||= Registry.new(self)
      end

      class Payload < Types::Data
        def [](key) = attributes[key]
        def fetch(...) = to_h.fetch(...)
      end

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

      def self.from(attrs)
        klass = registry[attrs[:type]]
        raise Sourced::UnknownMessageError, "Unknown message type: #{attrs[:type]}" unless klass

        klass.new(attrs)
      end

      def initialize(attrs = {})
        attrs = attrs.merge(payload: {}) unless attrs[:payload]
        super(attrs)
      end

      # Returns the payload attribute names for this message class.
      # Subclasses created via .define override this with a cached frozen array.
      def self.payload_attribute_names = EMPTY_ARRAY

      # Build QueryConditions for the intersection of this message's attributes
      # and the given key-value pairs.
      # Example:
      #   CourseCreated.to_conditions(course_name: 'Algebra', user_id: 'joe')
      #   # => [QueryCondition('course.created', 'course_name', 'Algebra')]
      #   # user_id is ignored because CourseCreated doesn't have it
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
      # Skips nil values. Returns array of [name, value] pairs.
      def extracted_keys
        return [] unless payload

        payload.to_h.filter_map { |k, v|
          [k.to_s, v.to_s] unless v.nil?
        }
      end
    end
  end
end
