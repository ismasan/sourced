# frozen_string_literal: true

require 'thread'

module Sourced
  module Backends
    class TestBackend
      class Group
        attr_reader :group_id

        Offset = Struct.new(:stream_id, :index, :locked)

        def initialize(group_id, backend)
          @group_id = group_id
          @backend = backend
          @offsets = {}
          reindex
        end

        def to_h
          active_offsets = @offsets.values.select { |o| o.index >= 0 }
          oldest_processed = (active_offsets.min_by(&:index)&.index || -1) + 1
          newest_processed = (active_offsets.max_by(&:index)&.index || -1) + 1
          stream_count = active_offsets.size

          { group_id:, oldest_processed:, newest_processed:, stream_count: }
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
            if block_given?
              yield(evt)
              offset.index = index
            end
            offset.locked = false
          end
          evt
        end

        private

        attr_reader :backend
      end

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
        attr_reader :events, :commands, :groups, :events_by_correlation_id, :events_by_stream_id, :stream_id_seq_index

        def initialize(
          events: [], 
          commands: [],
          groups: Hash.new { |h, k| h[k] = Group.new(k, self) }, 
          events_by_correlation_id: Hash.new { |h, k| h[k] = [] }, 
          events_by_stream_id: Hash.new { |h, k| h[k] = [] },
          stream_id_seq_index: {}
        )

          @events = events
          @groups = groups
          @events_by_correlation_id = events_by_correlation_id
          @commands = commands
          @events_by_stream_id = events_by_stream_id
          @stream_id_seq_index = stream_id_seq_index
        end

        def schedule_commands(commands)
          @commands = (@commands + commands).sort_by(&:created_at)
        end

        def next_command(&reserve)
          now = Time.now.utc

          if block_given?
            return nil if @commands.empty?
            return nil if @commands.first.created_at > now
            cmd = @commands.shift
            yield cmd
            cmd
          else
            @commands.first
          end
        end

        def copy
          self.class.new(
            events: events.dup,
            commands: commands.dup,
            groups: deep_dup(groups),
            events_by_correlation_id: deep_dup(events_by_correlation_id),
            events_by_stream_id: deep_dup(events_by_stream_id),
            stream_id_seq_index: deep_dup(stream_id_seq_index)
          )
        end

        private def deep_dup(hash)
          hash.each.with_object(hash.dup.clear) do |(k, v), new_hash|
            new_hash[k] = v.dup
          end
        end
      end

      def clear!
        @state = State.new
      end

      def installed? = true

      def reserve_next_for_reactor(reactor, &)
        group_id = reactor.consumer_info.group_id
        start_from = reactor.consumer_info.start_from.call
        transaction do
          group = @state.groups[group_id]
          group.reserve_next(reactor.handled_events, start_from, &)
        end
      end

      def ack_on(group_id, event_id, &)
        transaction do
          group = @state.groups[group_id]
          group.ack_on(event_id, &)
        end
      end

      def schedule_commands(commands)
        transaction do
          @state.schedule_commands(commands)
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
        groups = @state.groups.values.map(&:to_h).filter { |g| g[:stream_count] > 0 }
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
