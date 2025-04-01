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
          @offsets = {}
          @commands = []
          @status = :active
          @oldest_command_date = nil
          @error_context = {}
          @retry_at = nil
          reindex
        end

        def active? = @status == :active
        def active_with_commands? = active? && !!@oldest_command_date

        def stop(reason = nil)
          @error_context[:reason] = reason if reason
          @status = :stopped
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
          backend.events.each.with_index do |e, idx|
            @offsets[e.stream_id] ||= Offset.new(e.stream_id, -1, false)
          end
        end

        def ack_on(event_id, &)
          global_seq = backend.events.find_index { |e| e.id == event_id }
          return unless global_seq

          evt = backend.events[global_seq]
          offset = @offsets[evt.stream_id]
          if offset.locked
            raise Sourced::ConcurrentAckError, "Stream for event #{event_id} is being concurrently processed by #{group_id}" unless row
          else
            offset.locked = true
            yield
            offset.index = global_seq
            offset.locked = false
          end
        end

        NOOP_FILTER = ->(_) { true } 

        def reserve_next(handled_events, time_window, &)
          time_filter = time_window.is_a?(Time) ? ->(e) { e.created_at > time_window } : NOOP_FILTER
          evt = nil
          offset = nil
          index = -1

          backend.events.each.with_index do |e, idx|
            offset = @offsets[e.stream_id]
            if offset.locked # stream locked by another consumer in the group
              next
            elsif idx > offset.index && handled_events.include?(e.class) && time_filter.call(e) # new event for the stream
              evt = e
              offset.locked = true
              index = idx
              break
            else # event already consumed
            end
          end

          if evt
            if block_given? && yield(evt)
              # ACK reactor/event if block returns truthy
              offset.index = index
            end
            offset.locked = false
          end
          evt
        end

        private

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
        attr_reader :events, :groups, :events_by_correlation_id, :events_by_stream_id, :stream_id_seq_index

        def initialize(
          events: [], 
          groups: Hash.new { |h, k| h[k] = Group.new(k, self) }, 
          events_by_correlation_id: Hash.new { |h, k| h[k] = [] }, 
          events_by_stream_id: Hash.new { |h, k| h[k] = [] },
          stream_id_seq_index: {}
        )

          @events = events
          @groups = groups
          @events_by_correlation_id = events_by_correlation_id
          @command_locks = {}
          @events_by_stream_id = events_by_stream_id
          @stream_id_seq_index = stream_id_seq_index
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
            stream_id_seq_index: deep_dup(stream_id_seq_index)
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

      def reserve_next_for_reactor(reactor, &)
        group_id = reactor.consumer_info.group_id
        start_from = reactor.consumer_info.start_from.call
        transaction do
          group = @state.groups[group_id]
          if group.active? && (group.retry_at.nil? || group.retry_at <= Time.now)
            group.reserve_next(reactor.handled_events, start_from, &)
          end
        end
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

      def append_to_stream(stream_id, events)
        transaction do
          check_unique_seq!(events)

          events.each do |event|
            @state.events_by_correlation_id[event.correlation_id] << event
            @state.events_by_stream_id[stream_id] << event
            @state.events << event
            @state.stream_id_seq_index[seq_key(stream_id, event)] = true
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
