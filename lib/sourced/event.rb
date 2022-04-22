# frozen_string_literal: true

require 'time'
require 'dry-struct'

module Sourced
  # Time equality compares fractional seconds,
  # but Time#to_s (used by JSON.dump) doesn't preserve fractions,
  # so reconstituing an event with JSON.parse produces different times
  class ComparableTime < SimpleDelegator
    def self.utc
      new(Time.now.utc)
    end

    def self.wrap(obj)
      case obj
      when String
        new(Time.parse(obj))
      when Time, DateTime
        new(obj.to_time)
      when ComparableTime
        obj
      else
        raise ArgumentError, "#{obj.class} #{obj} cannot be coerced to Time"
      end
    end

    def ==(time)
      time.to_i == time.to_time.to_i
    end
  end

  module Types
    include Dry.Types()
    UUID = String.constrained(format: /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/.freeze)
    EventTime = Types.Constructor(ComparableTime){|v| ComparableTime.wrap(v) }.default { ComparableTime.utc }
  end

  class Event < Dry::Struct
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
    attribute :date, Types::EventTime
    attribute? :originator_id, Types::String.optional
    attribute? :payload do

    end

    def self.registry
      @registry ||= {}
    end

    def self.define(topic, &block)
      klass = Class.new(self) do
        def self.name
          'Sourced::Event'
        end
      end
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

    # ToDO: events should have a #metadata object
    # extensible by users, which would be copied when following.
    def self.follow(evt, payload = {})
      new(
        stream_id: evt.stream_id,
        originator_id: evt.id,
        payload: payload
      )
    end

    def copy(new_attrs = {})
      data = to_h.merge(new_attrs)
      self.class.new(data)
    end

    # Like #to_h
    # but include all keys defined in the struct
    # even optional ones with no value
    def hash_for_serialization
      self.class.schema.keys.map(&:name).each.with_object(to_h) do |k, ret|
        ret[k] = nil unless ret.key?(k)
      end
    end
  end
end
