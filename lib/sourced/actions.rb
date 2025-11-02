# frozen_string_literal: true

module Sourced
  module Actions
    RETRY = :retry
    OK = :ok

    # Append mesages to event store
    # using Backend#append_next_to_stream
    # which auto-increments stream sequence
    class AppendNext
      include Enumerable

      attr_reader :messages

      def initialize(messages)
        @messages = messages
        freeze
      end

      def ==(other)
        other.is_a?(self.class) && messages == other.messages
      end

      def each(&block)
        return enum_for(:each) unless block_given?

        messages.each do |message|
          block.call(message.stream_id, message)
        end
      end
    end

    # Append messages to a stream in event store
    # expecting messages to be in order
    # and with correct sequence numbers.
    # The backend will raise an error if mesages with same sequence
    # exist in the store (ie optimistic concurrency control).
    class AppendAfter
      attr_reader :stream_id, :messages

      def initialize(stream_id, messages)
        @stream_id = stream_id
        @messages = messages
      end
    end

    class Schedule
      attr_reader :messages, :at

      def initialize(messages, at:)
        @messages, @at = messages, at
      end
    end
  end
end
