# frozen_string_literal: true

require 'plumb'
require 'sourced/message'

module Sourced
  class Store
    # Serializes message payloads for {Sourced::Store}, which keeps the payload
    # as a JSON blob and the envelope in columns.
    #
    #   codec = Sourced::Store::MessageCodec.new
    #   codec.encode_payload(message)   # => JSON-native Hash (or nil)
    #   codec.decode(attrs)             # => the message, payload decoded
    #
    # It wraps a {Plumb::Codec} instance — a registry of +[decoder, encoder]+
    # pairs — holding each message class's *payload type* under its message type
    # string (+register('courses.created', CourseCreated::Payload)+). {#compile!}
    # builds it at boot and freezes it; a type missing from it raises. A payload
    # whose attributes are all JSON-native compiles to the payload class itself,
    # so it costs nothing extra. The envelope is the store's own business; see
    # {Store#append} and {Store#deserialize}.
    #
    # The format is {Plumb::Codec::JSON}: it decides that a +Date+ travels as
    # +"2026-01-02"+, and an app teaches it its own value types by registering
    # encoders on the class — +Plumb::Codec::JSON.encoder MoneyEncoder+ — before
    # {#compile!} runs.
    class MessageCodec
      # Raised when a stored payload no longer satisfies its message class's
      # schema — a schema change, a hand-edited row, a foreign writer.
      DecodeError = Class.new(Sourced::Error)

      # Raised when a message being written can't be represented in the codec's
      # format, which in practice means the message itself is invalid.
      EncodeError = Class.new(Sourced::Error)

      # The instance stores share. Holding no connections, it is safe to share,
      # so a process compiles its pairs once — including a store rebuilt after a
      # fork. Assign {Store#message_codec} to give a store its own.
      #
      # @return [MessageCodec]
      def self.default = @default ||= new

      # @param format [Class<Plumb::Codec>] the codec class compiled onto payload
      #   types. A seam for scoping a codec to its own format, as specs do; the
      #   format itself needs no configuring.
      # @param registry [Sourced::Message::Registry] resolves type strings to classes
      def initialize(format: Plumb::Codec::JSON, registry: Sourced::Message.registry)
        @format = format
        @registry = registry
        # Empty until #compile! builds the real one.
        @payloads = format.new { |_| }
      end

      # @return [String]
      def inspect = format('#<%s format=%s>', self.class.name, @format.name)

      # Build the registry: a payload pair for every message class, frozen once
      # they are all in. Registration happens here and nowhere else, so encoding
      # or decoding a type registered afterwards raises rather than compiling
      # mid-request.
      #
      # Also the boot check: a message type this codec can't represent raises,
      # naming the offending attribute path.
      #
      # @return [self]
      # @raise [Plumb::TypeError] if any registered message type can't be
      #   serialized by this codec
      def compile!
        @payloads = @format.new do |pairs|
          @registry.all { |klass| pairs.register(klass.type, payload_type(klass)) }
        end
        self
      end

      # @param type [String] message type string
      # @return [Boolean] whether a pair was compiled for this type
      def registered?(type) = @payloads.key?(type)

      # Encode a message's payload into JSON-native values.
      #
      # @param message [Sourced::Message]
      # @return [Hash, nil] nil for messages defined without a payload
      # @raise [Plumb::Codec::NoEntryError] if the type was not compiled
      # @raise [EncodeError] if the payload doesn't satisfy its schema
      def encode_payload(message)
        @payloads.encode(message.type, message.payload)
      rescue Plumb::ParseError => e
        raise EncodeError, "cannot encode #{label(message.type, message.id)}: #{e.message}"
      end

      # Rebuild a message from envelope attributes plus its still-encoded
      # payload. An unregistered message type raises: a process reading types it
      # doesn't know about is missing the class.
      #
      # @param attrs [Hash] symbol-keyed envelope attributes, +payload+ encoded
      # @return [Sourced::Message]
      # @raise [Sourced::Message::UnknownMessageError] if the type isn't in the
      #   message registry
      # @raise [Plumb::Codec::NoEntryError] if the type was not compiled
      # @raise [DecodeError] if the stored payload doesn't satisfy the schema
      def decode(attrs)
        type = attrs[:type]
        klass = @registry[type]
        raise Message::UnknownMessageError, "Unknown message type: #{label(type, attrs[:id])}" unless klass

        # Plumb structs short-circuit on instances of themselves, so handing over
        # the decoded Payload saves the message class re-parsing a hash.
        klass.new(attrs.merge(payload: @payloads.decode(type, attrs[:payload])))
      rescue Plumb::ParseError => e
        raise DecodeError, "cannot decode #{label(type, attrs[:id])}: #{e.message}"
      end

      private

      # The declared type of the message's +payload+ attribute, read from the
      # schema so that a message declaring a shared payload class
      # (+attribute :payload, SomeSharedPayload+) works too. A message defined
      # without a payload block declares +Static[nil]+, which the codec rewrites
      # to a nil pass-through both ways — no special case needed.
      def payload_type(klass)
        schema = klass._schema.to_h
        key = schema.keys.find { |k| k.to_sym == :payload }
        schema[key]
      end

      # "orders.placed (a1b2c3…)" — enough to find the offending row.
      def label(type, id) = "#{type} (#{id})"
    end
  end
end
