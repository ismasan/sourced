# frozen_string_literal: true

module Sourced
  # Actions represent the side effects produced by command/event handlers.
  # Each persistable action class implements {#execute}, which correlates messages
  # against a source message and persists them via the backend.
  # This provides a single place for the correlate-then-persist logic,
  # used by both backend reactors and {Sourced::Unit} synchronous processing.
  module Actions
    # Split a list of messages into
    # {AppendNext} or {Schedule} actions
    # based on their +created_at+ relative to now.
    #
    # @param messages [Array<Sourced::Message>]
    # @return [Array<AppendNext, Schedule>]
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

    # Append messages to the event store using Backend#append_next_to_stream,
    # which auto-increments stream sequence numbers.
    # Messages may target different streams and will be grouped by stream_id.
    class AppendNext
      include Enumerable

      # @return [Array<Sourced::Message>]
      attr_reader :messages

      # @param messages [Array<Sourced::Message>]
      def initialize(messages)
        @messages = messages
        freeze
      end

      def ==(other)
        other.is_a?(self.class) && messages == other.messages
      end

      # @yield [stream_id, message]
      # @yieldparam stream_id [String]
      # @yieldparam message [Sourced::Message]
      # @return [Enumerator] if no block given
      def each(&block)
        return enum_for(:each) unless block_given?

        messages.each do |message|
          block.call(message.stream_id, message)
        end
      end

      # Correlate messages against the source, then persist via backend.
      #
      # @param backend [#append_next_to_stream] the storage backend
      # @param source_message [Sourced::Message] message that caused this action
      # @return [Array<Sourced::Message>] correlated messages that were persisted
      def execute(backend, source_message)
        correlated = messages.map { |m| source_message.correlate(m) }
        correlated.group_by(&:stream_id).each do |stream_id, stream_messages|
          backend.append_next_to_stream(stream_id, stream_messages)
        end
        correlated
      end
    end

    # Append messages to a specific stream in the event store,
    # expecting messages to be in order and with correct sequence numbers.
    # The backend will raise {Sourced::ConcurrentAppendError} if messages
    # with the same sequence already exist (optimistic concurrency control).
    class AppendAfter
      # @return [String]
      attr_reader :stream_id
      # @return [Array<Sourced::Message>]
      attr_reader :messages

      # @param stream_id [String]
      # @param messages [Array<Sourced::Message>]
      def initialize(stream_id, messages)
        @stream_id = stream_id
        @messages = messages
      end

      # Correlate messages against the source, then persist via backend.
      #
      # @param backend [#append_to_stream] the storage backend
      # @param source_message [Sourced::Message] message that caused this action
      # @return [Array<Sourced::Message>] correlated messages that were persisted
      def execute(backend, source_message)
        correlated = messages.map { |m| source_message.correlate(m) }
        backend.append_to_stream(stream_id, correlated)
        correlated
      end
    end

    # Schedule messages for future delivery at a specific time.
    class Schedule
      # @return [Array<Sourced::Message>]
      attr_reader :messages
      # @return [Time]
      attr_reader :at

      # @param messages [Array<Sourced::Message>]
      # @param at [Time] when the messages should become available
      def initialize(messages, at:)
        @messages, @at = messages, at
      end

      # Correlate messages against the source, then schedule via backend.
      #
      # @param backend [#schedule_messages] the storage backend
      # @param source_message [Sourced::Message] message that caused this action
      # @return [Array<Sourced::Message>] correlated messages that were scheduled
      def execute(backend, source_message)
        correlated = messages.map { |m| source_message.correlate(m) }
        backend.schedule_messages(correlated, at: at)
        correlated
      end
    end

    # Execute a synchronous side effect (e.g. cache write, HTTP call)
    # within the current transaction. Does not persist messages.
    class Sync
      # @param work [#call] callable to execute
      def initialize(work)
        @work = work
      end

      def call(...) = @work.call(...)

      # Execute the work block. Backend and source_message are ignored.
      #
      # @param _backend [Object] unused
      # @param _source_message [Object] unused
      # @return [nil]
      def execute(_backend, _source_message)
        call
        nil
      end
    end
  end
end
