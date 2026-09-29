# frozen_string_literal: true

require 'sourced/message'
require 'sourced/message/json_codec'

module Sourced
  class Store
    # Serializes whole messages for {Sourced::Store}. The store writes the payload and
    # metadata as JSON blobs and the rest of the envelope to columns, and decodes a row
    # by handing all of it back through the codec — so metadata is decoded by its
    # schema, just as the payload is.
    #
    #   codec = Sourced::Store::MessageCodec.default.compile!
    #   codec.encode(message)   # => JSON-native Hash, String-keyed
    #   codec.decode(attrs)     # => the message
    #
    # Its own class, rather than {Sourced::Message::JSONCodec} itself, so a store's
    # codec has its own +.default+ and can be scoped to its own registry.
    class MessageCodec < Sourced::Message::JSONCodec
    end
  end
end
