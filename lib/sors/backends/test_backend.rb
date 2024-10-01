# frozen_string_literal: true

module Sors
  module Backends
    class TestBackend
      class Stream
        attr_reader :stream_id, :commands, :locked
        def initialize(stream_id)
          @stream_id = stream_id
          @commands = []
          @locked = false
        end

        def available? = !@locked && !@commands.empty?

        def <<(command)
          @commands << command
        end

        def reserve(&)
          @locked = true
          cmd = @commands.shift
          yield cmd if cmd
          @locked = false
        end
      end

      def initialize
        @command_streams = Hash.new { |h, k| h[k] = Stream.new(k) }
        @events = []
        @events_by_causation_id = Hash.new { |h, k| h[k] = [] }
      end

      def schedule_commands(commands)
        commands.each do |cmd|
          schedule_command(cmd.stream_id, cmd)
        end
        true
      end

      def schedule_command(stream_id, command)
        @command_streams[stream_id] << command
      end

      def reserve_next(&)
        stream = @command_streams.values.find(&:available?)
        stream&.reserve(&)
      end

      def transaction(&)
        yield
      end

      def append_events(events)
        @events.concat(events)
        events.each do |event|
          @events_by_causation_id[event.causation_id] << event
        end
        true
      end

      def read_event_batch(causation_id)
        @events_by_causation_id[causation_id]
      end

      def read_event_stream(stream_id)
      end
    end
  end
end
