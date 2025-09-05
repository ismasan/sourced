# frozen_string_literal: true

require 'thread'

module Sourced
  module Backends
    class TestBackend
      ACTIVE = 'active'
      STOPPED = 'stopped'

      class Group
        attr_reader :group_id, :commands, :oldest_command_date
        attr_accessor :status, :error_context, :retry_at

        Offset = Struct.new(:stream_id, :index, :locked)

        def initialize(group_id, backend)
          @group_id = group_id
          @backend = backend
          @commands = []
          @status = :active
          @oldest_command_date = nil
          @error_context = {}
          @retry_at = nil
          @highest_index = -1
          reset!
        end

        def active? = @status == :active
        def active_with_commands? = active? && !!@oldest_command_date

        def stop(reason = nil)
          @error_context[:reason] = reason if reason
          @status = :stopped
        end

        def reset!
          @offsets = {}
          reindex
        end

        def retry(time, ctx = {})
          @error_context.merge!(ctx)
          @retry_at = time
        end

        def to_h
          active_offsets = @offsets.values.select { |o| o.index >= 0 }
          oldest_processed = (active_offsets.min_by(&:index)&.index || -1) + 1
          newest_processed = (active_offsets.max_by(&:index)&.index || -1) + 1
          stream_count = active_offsets.size

          { 
            group_id:, 
            status: @status.to_s, 
            oldest_processed:, 
            newest_processed:, 
            stream_count:,
            retry_at:
          }
        end

        def schedule_commands(commands)
          @commands = (@commands + commands).sort_by(&:created_at)
          @oldest_command_date = @commands.first&.created_at
        end

        def delete_command(idx)
          @commands.delete_at(idx)
          @oldest_command_date = @commands.first&.created_at
        end

        def reindex
          backend.events.each do |e|
            @offsets[e.stream_id] ||= Offset.new(e.stream_id, -1, false)
          end
        end

        def ack_on(event_id, &)
          global_seq = backend.events.find_index { |e| e.id == event_id }
          return unless global_seq

          evt = backend.events[global_seq]
          offset = @offsets[evt.stream_id]
          if offset.locked
            raise Sourced::ConcurrentAckError, "Stream for event #{event_id} is being concurrently processed by #{group_id}"
          else
            offset.locked = true
            yield
            offset.index = global_seq
            @highest_index = global_seq if global_seq > @highest_index
            offset.locked = false
          end
        end

        NOOP_FILTER = ->(_) { true } 

        def reserve_next(handled_messages, time_window, process_action, &)
          time_filter = time_window.is_a?(Time) ? ->(e) { e.created_at > time_window } : NOOP_FILTER
          evt = nil
          offset = nil
          index = -1

          backend.events.each.with_index do |e, idx|
            offset = @offsets[e.stream_id]
            if offset.locked # stream locked by another consumer in the group
              next
            elsif idx > offset.index && handled_messages.include?(e.class) && time_filter.call(e) # new event for the stream
              evt = e
              offset.locked = true
              index = idx
              break
            else # event already consumed
            end
          end

          if evt
            replaying = @highest_index >= index
            if block_given?
              result = yield(evt, replaying)

              acker = -> { ack(offset, index) }
              process_action.(result, acker, evt)
            end

            offset.locked = false
          end

          evt
        end

        private

        def ack(offset, index)
          # ACK reactor/event
          offset.index = index
          @highest_index = index if index > @highest_index
        end

        attr_reader :backend
      end

      attr_reader :pubsub

      def initialize
        clear!
        @mutex = Mutex.new
        @in_tx = false
        @tx_id = nil
      end

      def events = @state.events

      def inspect
        %(<#{self.class} events:#{events.size} streams:#{@state.events_by_stream_id.size}>)
      end

      class State
        attr_reader :events, :groups, :events_by_correlation_id, :events_by_stream_id, :stream_id_seq_index, :streams, :scheduled_messages

        def initialize(
          events: [], 
          groups: Hash.new { |h, k| h[k] = Group.new(k, self) }, 
          events_by_correlation_id: Hash.new { |h, k| h[k] = [] }, 
          events_by_stream_id: Hash.new { |h, k| h[k] = [] },
          stream_id_seq_index: {},
          streams: {},
          scheduled_messages: []
        )

          @events = events
          @groups = groups
          @events_by_correlation_id = events_by_correlation_id
          @command_locks = {}
          @events_by_stream_id = events_by_stream_id
          @stream_id_seq_index = stream_id_seq_index
          @streams = streams
          @scheduled_messages = scheduled_messages
        end

        ScheduledMessageRecord = Data.define(:message, :at, :position) do
          def <=>(other)
            self.position <=> other.position
          end
        end

        Stream = Data.define(:stream_id, :seq, :updated_at) do
          def hash = stream_id
          def eql?(other) = other.is_a?(Stream) && stream_id == other.stream_id
        end

        def upsert_stream(stream_id, seq)
          str = Stream.new(stream_id, seq, Time.now)
          @streams[stream_id] = str
        end

        def schedule_messages(messages, at: Time.now)
          counter = @scheduled_messages.size
          records = messages.map do |a| 
            counter += 1
            ScheduledMessageRecord.new(a, at, [at, counter])
          end
          @scheduled_messages += records
          @scheduled_messages.sort!
        end

        def next_scheduled_messages(&)
          now = Time.now
          next_records, @scheduled_messages = @scheduled_messages.partition do |r|
            r.at <= now
          end
          next_messages = next_records.map(&:message)
          yield next_messages if next_messages.any?
          next_messages
        end

        def next_command(&reserve)
          now = Time.now
          group = @groups.values.filter(&:active_with_commands?).sort_by(&:oldest_command_date).first
          return nil unless group

          if block_given?
            idx = group.commands.index do |c|
              !@command_locks[c.stream_id] && c.created_at <= now
            end

            return nil unless idx

            cmd = group.commands[idx]
            @command_locks[cmd.stream_id] = true

            begin
              if yield(cmd)
                group.delete_command(idx)
              end
            ensure
              @command_locks.delete(cmd.stream_id)
            end
            cmd
          else
            group.commands.first
          end
        end

        def copy
          self.class.new(
            events: events.dup,
            groups: deep_dup(groups),
            events_by_correlation_id: deep_dup(events_by_correlation_id),
            events_by_stream_id: deep_dup(events_by_stream_id),
            stream_id_seq_index: deep_dup(stream_id_seq_index),
            streams: streams.dup,
            scheduled_messages: scheduled_messages.dup
          )
        end

        private

        def deep_dup(hash)
          hash.each.with_object(hash.dup.clear) do |(k, v), new_hash|
            new_hash[k] = v.dup
          end
        end
      end

      # An in-meory pubsub implementation for testing
      class TestPubSub
        def initialize
          @channels = {}
        end

        # @param channel_name [String]
        # @return [Channel]
        def subscribe(channel_name)
          @channels[channel_name] ||= Channel.new(channel_name)
        end

        # @param channel_name [String]
        # @param event [Sourced::Message]
        # @return [self]
        def publish(channel_name, event)
          channel = @channels[channel_name]
          channel&.publish(event)
          self
        end

        class Channel
          attr_reader :name

          def initialize(name)
            @name = name
            @handlers = []
          end

          def start(handler: nil, &block)
            handler ||= block
            @handlers << handler
          end

          def publish(event)
            @handlers.each do |handler|
              catch(:stop) do
                handler.call(event, self)
              end
            end
          end

          def stop = nil
        end
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
            group.reserve_next(reactor.handled_messages, start_from, method(:process_action), &)
          end
        end
      end

      private def process_action(action, ack, event)
        case action
        when Actions::OK
          ack.()

        when Actions::AppendNext
          correlate(event, action.messages).each do |msg|
            append_next_to_stream(msg.stream_id, msg)
          end
          ack.()

        when Actions::AppendAfter
          append_to_stream(action.stream_id, correlate(event, action.messages))
          ack.()

        when Actions::Schedule
          schedule_messages correlate(event, action.messages), at: action.at
          ack.()

        when Actions::RETRY
          # Don't ack

        else
          raise ArgumentError, "Unexpected Sourced::Actions type, but got: #{action.class}"
        end
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
        transaction do
          group = @state.groups[group_id]
          group.error_context = {}
          group.status = ACTIVE
          group.retry_at = nil
        end
      end

      def stop_consumer_group(group_id, error = nil)
        transaction do
          group = @state.groups[group_id]
          group.stop(error)
        end
      end

      def reset_consumer_group(group_id)
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
        transaction do
          @state.next_scheduled_messages do |scheduled_messages|
            scheduled_messages.each do |m|
              append_next_to_stream(m.stream_id, m)
            end
            scheduled_messages.size
          end
        end
      end

      def schedule_commands(commands, group_id:)
        transaction do
          group = @state.groups[group_id]
          group.schedule_commands(commands)
        end
      end

      def next_command(&)
        transaction do
          @state.next_command(&)
        end
      end

      Stats = Data.define(:stream_count, :max_global_seq, :groups)

      def stats
        stream_count = @state.events_by_stream_id.size
        max_global_seq = events.size
        groups = @state.groups.values.map(&:to_h)#.filter { |g| g[:stream_count] > 0 }
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
      # @param event [Sourced::Message] Event to append to the stream
      # @option max_retries [Integer] Not used in this backend, but kept for interface compatibility
      def append_next_to_stream(stream_id, event, max_retries: 3)
        transaction do
          last_event = @state.events_by_stream_id[stream_id].last
          last_seq = last_event ? last_event.seq : 0
          new_seq = last_seq + 1
          append_to_stream(stream_id, [event.with(seq: new_seq)])
        end
      end

      def append_to_stream(stream_id, events)
        transaction do
          check_unique_seq!(events)

          events.each do |event|
            @state.events_by_correlation_id[event.correlation_id] << event
            @state.events_by_stream_id[stream_id] << event
            @state.events << event
            @state.stream_id_seq_index[seq_key(stream_id, event)] = true
            @state.upsert_stream(stream_id, event.seq)
          end
        end
        @state.groups.each_value(&:reindex)
        true
      end

      def read_correlation_batch(event_id)
        event = @state.events.find { |e| e.id == event_id }
        return [] unless event
        @state.events_by_correlation_id[event.correlation_id]
      end

      def read_event_stream(stream_id, after: nil, upto: nil)
        events = @state.events_by_stream_id[stream_id]
        events = events.select { |e| e.seq > after } if after
        events = events.select { |e| e.seq <= upto } if upto
        events
      end

      private

      def check_unique_seq!(events)
        duplicate = events.find do |event|
          @state.stream_id_seq_index[seq_key(event.stream_id, event)]
        end
        if duplicate
          raise Sourced::ConcurrentAppendError, "Duplicate stream_id/seq: #{duplicate.stream_id}/#{duplicate.seq}"
        end
      end

      def seq_key(stream_id, event)
        [stream_id, event.seq]
      end
    end
  end
end
