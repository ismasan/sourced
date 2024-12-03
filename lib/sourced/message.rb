# frozen_string_literal: true

require 'sourced/types'

# A superclass and registry to define event types
# for example for an event-driven or event-sourced system.
# All events have an "envelope" set of attributes,
# including unique ID, stream_id, type, timestamp, causation ID,
# event subclasses have a type string (ex. 'users.name.updated') and an optional payload
# This class provides a `.define` method to create new event types with a type and optional payload struct,
# a `.from` method to instantiate the correct subclass from a hash, ex. when deserializing from JSON or a web request.
# and a `#follow` method to produce new events based on a previous event's envelope, where the #causation_id and #correlation_id
# are set to the parent event
# @example
#
#  # Define event struct with type and payload
#  UserCreated = Message.define('users.created') do
#    attribute :name, Types::String
#    attribute :email, Types::Email
#  end
#
#  # Instantiate a full event with .new
#  user_created = UserCreated.new(stream_id: 'user-1', payload: { name: 'Joe', email: '...' })
#
#  # Use the `.from(Hash) => Message` factory to lookup event class by `type` and produce the right instance
#  user_created = Message.from(type: 'users.created', stream_id: 'user-1', payload: { name: 'Joe', email: '...' })
#
#  # Use #follow(payload Hash) => Message to produce events following a command or parent event
#  create_user = CreateUser.new(...)
#  user_created = create_user.follow(UserCreated, name: 'Joe', email: '...')
#  user_created.causation_id == create_user.id
#  user_created.correlation_id == create_user.correlation_id
#  user_created.stream_id == create_user.stream_id
#
# ## JSON Schemas
# Plumb data structs support `.to_json_schema`, so you can document all events in the registry with something like
#
#   Message.registry.values.map(&:to_json_schema)
#
module Sourced
  class Message < Types::Data
    attribute :id, Types::AutoUUID
    attribute :stream_id, Types::String.present
    attribute :type, Types::String.present
    attribute :created_at, Types::Forms::Time.default { Time.now } #Types::JSON::AutoUTCTime
    attribute? :causation_id, Types::UUID::V4
    attribute? :correlation_id, Types::UUID::V4
    attribute :producer, Types::String.nullable.default(nil)
    attribute :seq, Types::Integer.default(1)
    attribute :metadata, Types::Hash.default(Plumb::BLANK_HASH)
    attribute :payload, Types::Static[nil]

    def self.registry
      @registry ||= {}
    end

    # Custom node_name to trigger specialiesed JSON Schema visitor handler.
    def self.node_name = :event

    def self.define(type_str, payload_schema: nil, &payload_block)
      type_str.freeze unless type_str.frozen?
      if registry[type_str]
        Sourced.config.logger.warn("Message '#{type_str}' already defined")
      end

      registry[type_str] = Class.new(self) do
        def self.node_name = :data
        define_singleton_method(:type) { type_str }

        attribute :type, Types::Static[type_str]
        if payload_schema
          attribute :payload, Types::Data[payload_schema]
        elsif block_given?
          attribute :payload, &payload_block if block_given?
        end
      end
    end

    def self.from(attrs)
      klass = registry[attrs[:type]]
      raise ArgumentError, "Unknown event type: #{attrs[:type]}" unless klass

      klass.new(attrs)
    end

    def follow(event_class, payload_attrs = nil)
      follow_with_attributes(
        event_class,
        payload: payload_attrs
      )
    end

    def follow_with_seq(event_class, seq, payload_attrs = nil)
      follow_with_attributes(
        event_class,
        attrs: { seq: },
        payload: payload_attrs
      )
    end

    def follow_with_stream_id(event_class, stream_id, payload_attrs = nil)
      follow_with_attributes(
        event_class,
        attrs: { stream_id: },
        payload: payload_attrs
      )
    end

    def follow_with_attributes(event_class, attrs: {}, payload: nil, metadata: nil)
      meta = self.metadata
      meta = meta.merge(metadata) if metadata
      attrs = { stream_id:, causation_id: id, correlation_id:, producer:, metadata: meta }.merge(attrs)
      attrs[:payload] = payload.to_h if payload
      event_class.parse(attrs)
    end

    def to_json(*)
      to_h.to_json(*)
    end

    private

    def prepare_attributes(attrs)
      attrs[:correlation_id] = attrs[:id] unless attrs[:correlation_id]
      attrs[:causation_id] = attrs[:id] unless attrs[:causation_id]
      attrs
    end
  end
end
