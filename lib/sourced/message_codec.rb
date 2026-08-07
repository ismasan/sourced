# frozen_string_literal: true

require 'sourced/message'
require 'sourced/codec'

module Sourced
  # Translates messages into JSON-native attribute hashes and back, so that
  # message classes can be declared with native Ruby types (+Date+, +Time+,
  # +Symbol+, +BigDecimal+, +URI+, +Range+, …) and still round-trip through a
  # JSON column.
  #
  #   codec = Sourced::MessageCodec.new
  #   data  = codec.dump(message)     # => JSON-native Hash
  #   codec.load(data)                # => the message, values decoded
  #
  # Codec compositions are deep type rewrites — roughly 0.3ms per message class
  # — so each class's +[decoder, encoder]+ pair is compiled once and cached.
  # {#compile!} warms the whole cache at boot (see {Configuration#setup!});
  # anything defined afterwards compiles lazily on first use.
  #
  # == Payload-scoped
  #
  # Pairs are compiled from the message's +payload+ attribute type, not from the
  # message class: the envelope (+id+, +type+, +created_at+, …) is mapped to
  # store columns by hand and needs no codec. This also keeps the common case
  # free — a payload whose attributes are all JSON-native rewrites to *itself*,
  # so the decoder is the payload class and costs nothing extra.
  class MessageCodec
    # A compiled pair. +decoder+ turns JSON-native input into payload values,
    # +encoder+ does the reverse.
    Pair = Data.define(:decoder, :encoder)

    # Raised when a stored payload no longer satisfies its message class's
    # schema — a schema change, a hand-edited row, a foreign writer.
    DecodeError = Class.new(Sourced::Error)

    # Raised when a message being written can't be represented in the codec's
    # format, which in practice means the message itself is invalid.
    EncodeError = Class.new(Sourced::Error)

    # The process-wide default, shared by stores built without an explicit
    # codec (+Store.new(db)+) and by a {Configuration} that was never given one.
    # Sharing it means one compiled cache per process rather than one per store.
    # @return [MessageCodec]
    def self.default = @default ||= new

    # @return [Class<Plumb::Codec>] the format this codec compiles onto messages
    attr_reader :codec

    # @param codec [Class<Plumb::Codec>] the wire format (default {Sourced::Codec})
    # @param registry [Sourced::Message::Registry] resolves type strings to classes
    def initialize(codec = Sourced::Codec, registry: Sourced::Message.registry)
      @codec = codec
      @registry = registry
      @pairs = {}
      @mutex = Mutex.new
    end

    # @return [String]
    def inspect = format('#<%s codec=%s compiled=%d>', self.class.name, @codec.name, @pairs.size)

    # Compile pairs for every registered message class. Called at boot (see
    # {Configuration#setup!}), where it does two jobs: it warms the cache so no
    # request pays for compilation, and it verifies that every message type the
    # process knows about can actually be stored.
    #
    # A message type this codec can't represent is a fatal misconfiguration —
    # it would raise the first time anything tried to append or read one — so
    # the +Plumb::TypeError+ propagates, naming the offending attribute path,
    # and the app fails to boot.
    #
    # @return [self]
    # @raise [Plumb::TypeError] if any registered message type can't be
    #   serialized by this codec
    def compile!
      @registry.all { |klass| pair_for(klass) }
      self
    end

    # The full JSON-native attribute hash for a message: envelope plus encoded
    # payload. Used for the scheduled-messages blob, where the whole message is
    # stored as one document.
    #
    # @param message [Sourced::Message]
    # @return [Hash]
    def dump(message)
      {
        id: message.id,
        type: message.type,
        causation_id: message.causation_id,
        correlation_id: message.correlation_id,
        created_at: message.created_at.iso8601(6),
        metadata: message.metadata,
        payload: encode_payload(message)
      }
    end

    # Just the encoded payload — what {Store#append} needs, since the envelope
    # goes into columns.
    #
    # @param message [Sourced::Message]
    # @return [Hash, nil] nil for messages defined without a payload
    # @raise [EncodeError] if the message's payload doesn't satisfy its schema
    def encode_payload(message)
      result = pair_for(message.class).encoder.resolve(message.payload)
      return result.value if result.valid?

      raise EncodeError, "cannot encode #{message.type} (#{message.id}): #{result.errors.inspect}"
    end

    # Rebuild a message from a JSON-decoded attribute hash, as produced by
    # {#dump}.
    #
    # An unregistered message type raises, as it does in {Message.from}: the
    # base {Sourced::Message} declares +payload+ as +Static[nil]+, so building
    # one would hand a reactor a message with its payload silently dropped.
    # A process reading types it doesn't know about is missing the class.
    #
    # @param attrs [Hash] symbol-keyed message attributes
    # @return [Sourced::Message]
    # @raise [Sourced::Message::UnknownMessageError] if the type isn't registered
    # @raise [DecodeError] if the stored payload doesn't satisfy the schema
    def load(attrs)
      klass = @registry[attrs[:type]]
      raise Message::UnknownMessageError, "Unknown message type: #{label(attrs)}" unless klass

      result = pair_for(klass).decoder.resolve(attrs[:payload])
      raise DecodeError, "cannot decode #{label(attrs)}: #{result.errors.inspect}" unless result.valid?

      # The decoded payload is a Payload instance, and Plumb structs
      # short-circuit on instances of themselves, so this is cheaper than
      # handing the message class a hash to re-parse.
      klass.new(attrs.merge(payload: result.value))
    end

    # The compiled pair for a message class, compiling it if needed.
    # @param klass [Class<Sourced::Message>]
    # @return [Pair]
    def pair_for(klass)
      @pairs[klass] || @mutex.synchronize { @pairs[klass] ||= build(klass) }
    end

    private

    def build(klass)
      Pair.new(*@codec.for(payload_type(klass)))
    end

    # "orders.placed (a1b2c3…)" — enough to find the offending row.
    def label(attrs) = "#{attrs[:type]} (#{attrs[:id]})"

    # The declared type of the message's +payload+ attribute. Read from the
    # schema rather than from a +Payload+ constant, so a message declaring a
    # shared payload class (+attribute :payload, SomeSharedPayload+) works too.
    #
    # A message defined without a payload block declares +Static[nil]+ here,
    # which the codec rewrites to a nil pass-through in both directions — so
    # there's no special case for payload-less messages.
    def payload_type(klass)
      schema = klass._schema.to_h
      key = schema.keys.find { |k| k.to_sym == :payload }
      schema[key]
    end
  end
end
