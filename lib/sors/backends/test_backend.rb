# frozen_string_literal: true

require 'thread'

module Sors
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

        def reserve_next(&)
          evt = nil
          offset = nil
          index = -1

          backend.events.each.with_index do |e, idx|
            offset = @offsets[e.stream_id]
            if offset.locked # stream locked by another consumer in the group
              next
            elsif idx > offset.index # new event for the stream
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

      # These are not part of the Backend interface
      # but are convenient to inspect the TestBackend
      attr_reader :events

      def initialize
        @events = []
        @groups = Hash.new { |h, k| h[k] = Group.new(k, self) }
        @events_by_causation_id = Hash.new { |h, k| h[k] = [] }
        @events_by_stream_id = Hash.new { |h, k| h[k] = [] }
        @stream_id_seq_index = {}
        @mutex = Mutex.new
        @in_tx = false
      end

      def installed? = true

      def reserve_next_for(group_id, &)
        transaction do
          group = @groups[group_id]
          group.reserve_next(&)
        end
      end

      Stats = Data.define(:stream_count, :max_global_seq, :groups)

      def stats
        stream_count = @events_by_stream_id.size
        max_global_seq = events.size
        groups = @groups.values.map(&:to_h).filter { |g| g[:stream_count] > 0 }
        Stats.new(stream_count, max_global_seq, groups)
      end

      def transaction(&)
        if @in_tx
          yield
        else
          @mutex.synchronize do
            @in_tx = true
            result = yield
            @in_tx = false
            result
          end
        end
      end

      def append_to_stream(stream_id, events)
        transaction do
          check_unique_seq!(events)

          events.each do |event|
            @events_by_causation_id[event.causation_id] << event
            @events_by_stream_id[stream_id] << event
            @events << event
            @stream_id_seq_index[seq_key(stream_id, event)] = true
          end
        end
        @groups.each_value(&:reindex)
        true
      end

      def read_event_batch(causation_id)
        @events_by_causation_id[causation_id]
      end

      def read_event_stream(stream_id, after: nil, upto: nil)
        events = @events_by_stream_id[stream_id]
        events = events.select { |e| e.seq > after } if after
        events = events.select { |e| e.seq <= upto } if upto
        events
      end

      private

      def check_unique_seq!(events)
        duplicate = events.find do |event|
          @stream_id_seq_index[seq_key(event.stream_id, event)]
        end
        if duplicate
          raise Sors::ConcurrentAppendError, "Duplicate stream_id/seq: #{duplicate.stream_id}/#{duplicate.seq}"
        end
      end

      def seq_key(stream_id, event)
        [stream_id, event.seq]
      end
    end
  end
end
