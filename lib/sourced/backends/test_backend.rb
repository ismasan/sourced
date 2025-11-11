# frozen_string_literal: true

require 'thread'

module Sourced
  module Backends
    class TestBackend
      ACTIVE = 'active'
      STOPPED = 'stopped'

      attr_reader :pubsub

      def initialize
        clear!
        @mutex = Mutex.new
        @in_tx = false
        @tx_id = nil
      end

      def messages = @state.messages

      def inspect
        %(<#{self.class} messages:#{messages.size} streams:#{@state.messages_by_stream_id.size}>)
      end

      def clear!
        @state = State.new
        @pubsub = TestPubSub.new
      end

      def installed? = true

      def handling_reactor_exceptions(_reactor, &)
        yield
      end

      def reserve_next_for_reactor(reactor, worker_id: Process.pid.to_s, &)
        group_id = reactor.consumer_info.group_id
        start_from = reactor.consumer_info.start_from.call
        transaction do
          group = @state.groups[group_id]
          if group.active? && (group.retry_at.nil? || group.retry_at <= Time.now)
            group.reserve_next(reactor.handled_messages, start_from, method(:process_actions), &)
          end
        end
      end

      private def process_actions(group_id, actions, ack, event, offset)
        should_ack = false
        actions = [actions] unless actions.is_a?(Array)
        actions = actions.compact
        # Empty actions is assumed to be an ACK
        return ack.() if actions.empty?

        actions.each do |action|
          case action
          when Actions::OK
            should_ack = true

          when Actions::Ack
            offset.locked = false
            ack_on(group_id, action.message_id)

          when Actions::AppendNext
            messages = correlate(event, action.messages)
            messages.group_by(&:stream_id).each do |stream_id, stream_messages|
              append_next_to_stream(stream_id, stream_messages)
            end
            should_ack = true

          when Actions::AppendAfter
            append_to_stream(action.stream_id, correlate(event, action.messages))
            should_ack = true

          when Actions::Schedule
            schedule_messages correlate(event, action.messages), at: action.at
            should_ack = true

          when Actions::Sync
            action.call
            should_ack = true

          when Actions::RETRY
            # Don't ack

          else
            raise ArgumentError, "Expected Sourced::Actions type, but got: #{action.class}"
          end
        end

        ack.() if should_ack
      end

      private def correlate(source_message, messages)
        messages.map { |e| source_message.correlate(e) }
      end

      def ack_on(group_id, event_id, &)
        transaction do
          group = @state.groups[group_id]
          group.ack_on(event_id, &)
        end
      end

      def register_consumer_group(group_id)
        transaction do
          @state.groups[group_id]
        end
      end

      def updating_consumer_group(group_id, &)
        transaction do
          group = @state.groups[group_id]
          yield group
        end
      end

      # @param group_id [String]
      def start_consumer_group(group_id)
        group_id = group_id.consumer_info.group_id if group_id.respond_to?(:consumer_info)
        transaction do
          group = @state.groups[group_id]
          group.error_context = {}
          group.status = ACTIVE
          group.retry_at = nil
        end
      end

      def stop_consumer_group(group_id, error = nil)
        group_id = group_id.consumer_info.group_id if group_id.respond_to?(:consumer_info)
        transaction do
          group = @state.groups[group_id]
          group.stop(error)
        end
      end

      def reset_consumer_group(group_id)
        group_id = group_id.consumer_info.group_id if group_id.respond_to?(:consumer_info)
        transaction do
          group = @state.groups[group_id]
          group.reset!
        end
        true
      end

      def schedule_messages(messages, at: Time.now)
        @state.schedule_messages(messages, at:)
        true
      end

      def update_schedule!
        count = 0
        transaction do
          @state.next_scheduled_messages do |scheduled_messages|
            scheduled_messages.group_by(&:stream_id).each do |stream_id, stream_messages|
              append_next_to_stream(stream_id, stream_messages)
            end
            count = scheduled_messages.size
          end
          count
        end
      end

      Stats = Data.define(:stream_count, :max_global_seq, :groups)

      def stats
        stream_count = @state.messages_by_stream_id.size
        max_global_seq = messages.size
        groups = @state.groups.values.map(&:to_h)
        Stats.new(stream_count, max_global_seq, groups)
      end

      # Retrieve a list of recently active streams, ordered by most recent activity.
      # This is the in-memory implementation that maintains stream metadata during testing.
      #
      # @param limit [Integer] Maximum number of streams to return (defaults to 10)
      # @return [Array<Stream>] Array of Stream objects ordered by updated_at descending
      # @see SequelBackend#recent_streams
      def recent_streams(limit: 10)
        # Input validation (consistent with SequelBackend)
        return [] if limit == 0
        raise ArgumentError, "limit must be a positive integer" if limit < 0
        
        @state.streams.values.sort_by(&:updated_at).reverse.take(limit)
      end

      def transaction(&)
        if @in_tx
          yield
        else
          @mutex.synchronize do
            @in_tx = true
            @state_snapshot = @state.copy
            result = yield
            @in_tx = false
            @state_snapshot = nil
            result
          end
        end
      rescue StandardError => e
        @in_tx = false
        @state = @state_snapshot if @state_snapshot
        raise
      end

      # @param stream_id [String] Unique identifier for the event stream
      # @param messages [Sourced::Message, Array<Sourced::Message>] Event(s) to append to the stream
      # @option max_retries [Integer] Not used in this backend, but kept for interface compatibility
      def append_next_to_stream(stream_id, messages, max_retries: 3)
        # Handle both single event and array of messages
        messages_array = Array(messages)
        return true if messages_array.empty?

        transaction do
          last_message = @state.messages_by_stream_id[stream_id].last
          last_seq = last_message ? last_message.seq : 0
          
          messages_with_seq = messages_array.map.with_index do |message, index|
            message.with(seq: last_seq + index + 1)
          end
          
          append_to_stream(stream_id, messages_with_seq)
        end
      end

      def append_to_stream(stream_id, messages)
        # Handle both single event and array of events
        messages_array = Array(messages)
        return false if messages_array.empty?

        transaction do
          check_unique_seq!(messages_array)

          messages_array.each do |message|
            @state.messages_by_correlation_id[message.correlation_id] << message
            @state.messages_by_stream_id[stream_id] << message
            @state.messages << message
            @state.stream_id_seq_index[seq_key(stream_id, message)] = true
            @state.upsert_stream(stream_id, message.seq)
          end
        end
        @state.groups.each_value(&:reindex)
        true
      end

      def read_correlation_batch(message_id)
        message = @state.messages.find { |e| e.id == message_id }
        return [] unless message
        @state.messages_by_correlation_id[message.correlation_id]
      end

      def read_stream(stream_id, after: nil, upto: nil)
        messages = @state.messages_by_stream_id[stream_id]
        messages = messages.select { |e| e.seq > after } if after
        messages = messages.select { |e| e.seq <= upto } if upto
        messages
      end

      # No-op heartbeats for test backend
      def worker_heartbeat(worker_ids, at: Time.now)
        Array(worker_ids).size
      end

      # No-op stale claim release for test backend
      def release_stale_claims(ttl_seconds: 120)
        0
      end

      private

      def check_unique_seq!(messages)
        duplicate = messages.find do |message|
          @state.stream_id_seq_index[seq_key(message.stream_id, message)]
        end
        if duplicate
          raise Sourced::ConcurrentAppendError, "Duplicate stream_id/seq: #{duplicate.stream_id}/#{duplicate.seq}"
        end
      end

      def seq_key(stream_id, message)
        [stream_id, message.seq]
      end
    end
  end
end

require 'sourced/backends/test_backend/group'
require 'sourced/backends/test_backend/state'
require 'sourced/backends/test_backend/test_pub_sub'
