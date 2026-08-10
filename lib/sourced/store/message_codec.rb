# frozen_string_literal: true

require 'sourced/message'
require 'sourced/message/json_codec'

module Sourced
  class Store
    # Serializes message payloads for {Sourced::Store}, which keeps the payload as a
    # JSON blob and the envelope in columns.
    #
    #   codec = Sourced::Store::MessageCodec.default.compile!
    #   codec.encode(message)   # => JSON-native Hash (or nil)
    #   codec.decode(attrs)     # => the message, payload decoded
    #
    # A {Sourced::Message::JSONCodec} that compiles each message class's *payload type*
    # instead of the whole class (+register('courses.created', CourseCreated::Payload)+),
    # because the envelope goes to columns — see {Store#append} and {Store#deserialize}.
    # A payload whose attributes are all JSON-native compiles to the payload class
    # itself, so it costs nothing extra. A message declared without a payload compiles
    # its +Static[nil]+ declaration, and encodes to +nil+.
    #
    # Everything else — the compiled-pair registry, the pair cache, +compile!+,
    # +decode+, the error classes — comes from the base class.
    class MessageCodec < Sourced::Message::JSONCodec
      private

      # The payload schema, read off the message class so that a message declaring a
      # shared payload class (+attribute :payload, SomeSharedPayload+) works too. A
      # message defined without a payload declares +Static[nil]+, which the codec
      # rewrites to a nil pass-through both ways — no special case needed.
      def compiled_type(klass)
        schema = klass._schema.to_h
        key = schema.keys.find { |k| k.to_sym == :payload }
        schema[key]
      end

      # Only the payload is encoded; the store writes the envelope itself.
      def encode_subject(message) = message.payload

      # The decoded payload is handed over as an instance rather than a Hash: Plumb
      # structs short-circuit on instances of themselves, so the message class does not
      # re-parse it.
      def build(klass, attrs, decoder)
        klass.new(attrs.merge(payload: decoder.parse(attrs[:payload])))
      end
    end
  end
end
