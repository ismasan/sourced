# frozen_string_literal: true

require 'plumb'

module Sourced
  # The default wire format for messages: Plumb's JSON codec, plus Sourced's
  # own precision requirements.
  #
  # Because messages are declared with native Ruby types, the codec is what
  # turns them into JSON-native structures for the store and back again:
  #
  #   Scheduled = Sourced::Event.define('courses.scheduled') do
  #     attribute :starts_on, Date
  #     attribute :published_at, Time
  #   end
  #
  # Out of the box this covers Date, Time, Symbol, BigDecimal, URI and Range,
  # alongside every JSON-native scalar, hash, array, nested struct, union and
  # nullable. Anything else needs an encoder, registered on a subclass:
  #
  #   class MyCodec < Sourced::Codec
  #     encoder MoneyEncoder
  #   end
  #
  #   Sourced.configure { |config| config.codec = MyCodec }
  #
  # @see Sourced::MessageCodec which compiles this codec onto message classes
  class Codec < Plumb::Codec::JSON
    # Plumb's built-in TimeEncoder serializes with Time#iso8601, which truncates
    # to whole seconds. Messages in an event log are ordered in time and often
    # compared below that resolution, so truncating on write loses information
    # the log is supposed to keep. Same input type as the encoder it replaces
    # (the ISO 8601 pattern allows a fractional part), so this subclass wins
    # most-specific matching over the inherited one.
    class TimeEncoder < Plumb::Codec::TimeEncoder
      def encode(time) = time.iso8601(6)
    end

    encoder TimeEncoder
  end
end
