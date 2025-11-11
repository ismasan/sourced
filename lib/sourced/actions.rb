# frozen_string_literal: true

module Sourced
  module Actions
    # Split a list of messages into 
    # Actions::AppendNext or 
    # Actions::Schedule
    # based on their #created_at
    def self.build_for(messages)
      actions = []
      return actions if messages.empty?

      # TODO: I really need a uniform Clock object
      now = Time.now
      to_schedule, to_append = messages.partition { |e| e.created_at > now }
      actions << AppendNext.new(to_append) if to_append.any?
      to_schedule.group_by(&:created_at).each do |at, msgs|
        actions << Schedule.new(msgs, at:)
      end

      actions
    end

    RETRY = :retry
    OK = :ok

    # ACK an arbitrary message ID
    Ack = Data.define(:message_id)

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

    class Sync
      def initialize(work)
        @work = work
      end

      def call(...) = @work.call(...)
    end
  end
end
