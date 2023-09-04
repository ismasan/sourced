# frozen_string_literal: true

require 'time'
require 'dry-struct'

module Sourced
  module Types
    include Dry.Types()
    UUID = String.constrained(format: /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.freeze)
    # DateTime = Types.Constructor(ComparableTime) { |v| ComparableTime.wrap(v) }
    # EventTime = DateTime.default { ComparableTime.utc }
    # DateTime = Types::JSON::DateTime
    EventTime = Types::JSON::Time.default { ::Time.now.utc }
  end

  class Event < Dry::Struct
    EMPTY_HASH = Hash.new.freeze

    class BasePayload < Dry::Struct
      def self.name
        'Sourced::Event::Payload'
      end

      transform_keys(&:to_sym)
    end

    transform_keys(&:to_sym)

    attribute :topic, Types::String
    attribute :id, Types::UUID.default { SecureRandom.uuid }
    attribute? :stream_id, Types::String
    attribute :seq, Types::Integer.default(1)
    attribute :created_at, Types::EventTime
    attribute :metadata, Types::Hash.default(EMPTY_HASH)
    attribute? :payload do

    end

    def self.registry
      @registry ||= {}
    end

    def self.define(topic, class_metadata = {}, &block)
      klass = Class.new(self) do
        def self.name
          'Sourced::Event'
        end
      end
      klass.define_singleton_method(:meta) { class_metadata.freeze }
      klass.define_singleton_method(:topic) { topic }
      klass.attribute :topic, Types.Value(topic).default(topic.freeze)
      if block_given?
        payload_class = Class.new(BasePayload)
        payload_class.instance_eval(&block)
        klass.attribute :payload, payload_class
      end

      ::Sourced::Event.registry[topic] = klass
    end

    def self.resolve(topic)
      klass = ::Sourced::Event.registry[topic]
      raise UnknownEventError, "no event schema registered for '#{topic}'" unless klass
      klass
    end

    def self.from(data = {})
      data[:topic] = topic unless data[:topic]
      klass = resolve(data[:topic])
      klass.new(data)
    end

    def self.follow(evt, payload = {})
      new(
        stream_id: evt.stream_id,
        metadata: evt.metadata.merge(causation_id: evt.id, correlation_id: evt.correlation_id),
        payload: payload
      )
    end

    def causation_id
      metadata.key?(:causation_id) ? metadata[:causation_id] : id
    end

    def correlation_id
      metadata.key?(:correlation_id) ? metadata[:correlation_id] : id
    end

    def copy(new_attrs = {})
      data = to_h.merge(new_attrs)
      self.class.new(data)
    end

    def to_h
      super.tap do |hash|
        hash[:metadata][:causation_id] ||= id
        hash[:metadata][:correlation_id] ||= id
      end
    end

    # Like #to_h
    # but include all keys defined in the struct
    # even optional ones with no value
    def hash_for_serialization
      self.class.schema.keys.map(&:name).each.with_object(to_h) do |k, ret|
        ret[k] = nil unless ret.key?(k)
      end.merge(created_at: created_at&.iso8601(6))
    end

    def as_json(...)
      to_h.merge(created_at: created_at.iso8601(6))
    end
  end
end
