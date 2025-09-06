# frozen_string_literal: true

module Sourced
  module Backends
    class TestBackend
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

    end
  end
end
