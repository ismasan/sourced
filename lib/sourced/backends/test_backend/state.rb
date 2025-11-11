# frozen_string_literal: true

module Sourced
  module Backends
    class TestBackend
      class State
        attr_reader :messages, :groups, :messages_by_correlation_id, :messages_by_stream_id, :stream_id_seq_index, :streams, :scheduled_messages

        def initialize(
          messages: [], 
          groups: Hash.new { |h, k| h[k] = Group.new(k, self) }, 
          messages_by_correlation_id: Hash.new { |h, k| h[k] = [] }, 
          messages_by_stream_id: Hash.new { |h, k| h[k] = [] },
          stream_id_seq_index: {},
          streams: {},
          scheduled_messages: []
        )

          @messages = messages
          @groups = groups
          @messages_by_correlation_id = messages_by_correlation_id
          @messages_by_stream_id = messages_by_stream_id
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

        def copy
          self.class.new(
            messages: messages.dup,
            groups: deep_dup(groups),
            messages_by_correlation_id: deep_dup(messages_by_correlation_id),
            messages_by_stream_id: deep_dup(messages_by_stream_id),
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
