# frozen_string_literal: true

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
    # The format — the {Plumb::Codec} deciding that a +Date+ travels as
    # +"2026-01-02"+ and that your +Money+ has an encoder — is global, set with
    # +config.codec=+ and reaching this class through {Store#codec=}.
    class MessageCodec
      # Raised when a stored payload no longer satisfies its message class's
      # schema — a schema change, a hand-edited row, a foreign writer.
      DecodeError = Class.new(Sourced::Error)

      # Raised when a message being written can't be represented in the codec's
      # format, which in practice means the message itself is invalid.
      EncodeError = Class.new(Sourced::Error)

      # The shared instance for a format. Holding no connections, it is safe to
      # share, so stores on the same format compile their pairs once — including
      # a store rebuilt after a fork. Assign {Store#message_codec} to give a
      # store its own.
      #
      # @param codec [Class<Plumb::Codec>]
      # @return [MessageCodec]
      def self.for(codec)
        @instances ||= {}
        @instances[codec] ||= new(codec)
      end

      # @return [MessageCodec] the shared instance for the default format
      def self.default = self.for(Plumb::Codec::JSON)

      # @return [Class<Plumb::Codec>] the format compiled onto payload types
      attr_reader :codec

      # @param codec [Class<Plumb::Codec>] the wire format (default {Plumb::Codec::JSON})
      # @param registry [Sourced::Message::Registry] resolves type strings to classes
      def initialize(codec = Plumb::Codec::JSON, registry: Sourced::Message.registry)
        @codec = codec
        @registry = registry
        # Empty until #compile! builds the real one.
        @payloads = codec.new { |_| }
      end

      # @return [String]
      def inspect = format('#<%s codec=%s>', self.class.name, @codec.name)

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
        @payloads = @codec.new do |pairs|
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
