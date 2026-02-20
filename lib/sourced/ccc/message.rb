# frozen_string_literal: true

require 'sourced/types'

module Sourced
  module CCC
    QueryCondition = Data.define(:message_type, :key_name, :key_value)
    ConsistencyGuard = Data.define(:conditions, :last_position)

    class Message < Types::Data
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
            const_set(:Payload, Class.new(Payload, &payload_block))
            attribute :payload, self::Payload
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
