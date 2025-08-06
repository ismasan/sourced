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
# ## Message registries
# Each Message class has its own registry of sub-classes.
# You can use the top-level Sourced::Message.from(hash) to instantiate all message types.
# You can also scope the lookup by sub-class.
# 
# @example
#
#   class PublicCommand < Sourced::Message; end
#   
#   DoSomething = PublicCommand.define('commands.do_something')
#
#   # Use .from scoped to PublicCommand subclass
#   # to ensure that only PublicCommand subclasses are accessible.
#   cmd = PublicCommand.from(type: 'commands.do_something', payload: { ... })
#
# ## JSON Schemas
# Plumb data structs support `.to_json_schema`, so you can document all events in the registry with something like
#
#   Message.subclasses.map(&:to_json_schema)
#
module Sourced
  UnknownMessageError = Class.new(ArgumentError)
  PastMessageDateError = Class.new(ArgumentError)

  class Message < Types::Data
    attribute :id, Types::AutoUUID
    attribute :stream_id, Types::String.present
    attribute :type, Types::String.present
    attribute :created_at, Types::Forms::Time.default { Time.now } #Types::JSON::AutoUTCTime
    attribute? :causation_id, Types::UUID::V4
    attribute? :correlation_id, Types::UUID::V4
    attribute :seq, Types::Integer.default(1)
    attribute :metadata, Types::Hash.default(Plumb::BLANK_HASH)
    attribute :payload, Types::Static[nil]

    class Registry
      def initialize(message_class)
        @message_class = message_class
        @lookup = {}
      end

      def []=(key, klass)
        @lookup[key] = klass
      end

      def [](key)
        klass = lookup[key]
        return klass if klass

        message_class.subclasses.each do |c|
          klass = c.registry[key]
          return klass if klass
        end
        nil
      end

      def inspect
        %(<#{self.class}:#{object_id} #{lookup.size} keys, #{message_class.subclasses.size} child registries>)
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
          attribute :payload, Payload[payload_schema]
        elsif block_given?
          attribute :payload, Payload, &payload_block if block_given?
        end
      end
    end

    def self.from(attrs)
      klass = registry[attrs[:type]]
      raise UnknownMessageError, "Unknown event type: #{attrs[:type]}" unless klass

      klass.new(attrs)
    end

    def initialize(attrs = {})
      unless attrs[:payload]
        attrs = attrs.merge(payload: {})
      end
      super(attrs)
    end

    def with_metadata(meta = {})
      return self if meta.empty?

      attrs = metadata.merge(meta)
      with(metadata: attrs)
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
      attrs = { stream_id:, causation_id: id, correlation_id:, metadata: meta }.merge(attrs)
      attrs[:payload] = payload.to_h if payload
      event_class.parse(attrs)
    end

    def correlate(message)
      attrs = {
        causation_id: id,
        correlation_id:,
        metadata: metadata.merge(message.metadata || Plumb::BLANK_HASH)
      }
      message.with(attrs)
    end

    def delay(datetime)
      if datetime < created_at
        raise PastMessageDateError, "Message #{type} can't be delayed to a date in the past"
      end
      with(created_at: datetime)
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

  class Command < Message; end
  class Event < Message; end
end
