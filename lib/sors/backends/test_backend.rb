# frozen_string_literal: true

require 'thread'

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
          cmd
        end
      end

      # These are not part of the Backend interface
      # but are convenient to inspect the TestBackend
      attr_reader :command_streams, :events

      def initialize
        @command_streams = Hash.new { |h, k| h[k] = Stream.new(k) }
        @events = []
        @events_by_causation_id = Hash.new { |h, k| h[k] = [] }
        @mutex = Mutex.new
        @in_tx = false
      end

      def installed? = true

      def schedule_commands(commands)
        transaction do
          commands.each do |cmd|
            schedule_command(cmd.stream_id, cmd)
          end
          true
        end
      end

      def schedule_command(stream_id, command)
        @command_streams[stream_id] << command
      end

      def reserve_next(&)
        transaction do
          stream = @command_streams.values.find(&:available?)
          stream&.reserve(&)
        end
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

      def append_events(events)
        transaction do
          @events.concat(events)
          events.each do |event|
            @events_by_causation_id[event.causation_id] << event
          end
        end
        true
      end

      def read_event_batch(causation_id)
        @events_by_causation_id[causation_id]
      end

      def read_event_stream(stream_id)
        @events.select { |e| e.stream_id == stream_id }
      end
    end
  end
end
