# frozen_string_literal: true

require 'sors/router'
require 'sors/message'

module Sors
  class Aggregate
    include Decide
    include Evolve
    include React

    class << self
      # Register as a Reactor
      def handled_events = self.handled_events_for_react

      # The Reactor interface
      # @param events [Array<Message>]
      def handle_events(events)
        load(events.first.stream_id).handle_events(events)
      end

      # The Decider interface
      # @param cmd [Message]
      def handle_command(cmd)
        load(cmd.stream_id).handle_command(cmd)
      end

      # Create a new Aggregate instance
      #
      # @param stream_id [String] the stream id
      # @return [Aggregate]
      def create(stream_id = SecureRandom.uuid)
        new(stream_id)
      end

      # Load an Aggregate from event history
      #
      # @param stream_id [String] the stream id
      # @return [Aggregate]
      def load(stream_id)
        new(stream_id).load
      end
    end

    attr_reader :id, :logger, :seq

    def initialize(id)
      @id = id
      @seq = 0
      @logger = Sors.config.logger
      @backend = Sors.config.backend
    end

    def ==(other)
      other.is_a?(self.class) && id == other.id && seq == other.seq
    end

    def handle_command(command)
      logger.info "Handling #{command.type}"
      events = decide(command)
      evolve(events)
      transaction do
        events = save(command, events)
        # Schedule a system command to handle this batch of events in the background
        schedule_batch(command)
      end
      [ self, events ]
    end

    # Reactor interface
    def handle_events(events, &map_commands)
      commands = react(events)
      commands = commands.map(&map_commands) if map_commands
      schedule_commands(commands)
    end

    def load
      events = backend.read_event_stream(id)
      @seq = events.last&.seq || 0
      evolve(events)
      self
    end

    def save(command, events)
      # Update :seq for each event based on seq
      # TODO: we do the same in Machine#save. DRY this up
      events = [command, *events].map do |event|
        @seq += 1
        event.with(seq: @seq)
      end
      backend.append_events(events)
      events
    end

    private

    attr_reader :backend

    def command(klass, payload = {})
      handle_command klass.new(stream_id: id, payload:)
    end

    def schedule_batch(command, commands = [])
      schedule_commands([command.follow(ProcessBatch), *commands])
    end

    def schedule_commands(commands)
      backend.schedule_commands(commands)
    end

    def transaction(&)
      backend.transaction(&)
    end
  end
end
