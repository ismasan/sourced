# frozen_string_literal: true

module Sors
  class Machine
    extend Consumer
    include Decide
    include Evolve
    include React
    include Sync

    attr_reader :backend

    class << self
      # Register as a Reactor
      def handled_events = self.handled_events_for_react

      # The Reactor interface
      # @param events [Array<Message>]
      def handle_events(events)
        new.handle_events(events)
      end

      # The Decider interface
      # @param cmd [Message]
      def handle_command(cmd)
        new.handle_command(cmd)
      end
    end

    attr_reader :seq

    def initialize(logger: Sors.config.logger, backend: Sors.config.backend)
      @logger = logger
      @backend = backend
      @seq = 0
    end

    def inspect
      %(<#{self.class}:#{object_id} backend:#{backend.inspect}>)
    end

    def ==(other)
      other.is_a?(self.class) && other.backend == backend
    end

    # TODO: perhaps the only difference between a Machine and an Aggregate
    # is how they initialise state
    # Machine returns a single state object
    # Aggregate sets up internal @ivars
    # They could be a single class that choses how to initialise state
    # depending on what method it implements.
    # Ex.
    #   def init_state(stream_id)
    #     Cart.new(stream_id)
    #   end
    #
    #   def setup_state(stream_id)
    #     @items = {}
    #   end
    def init_state(stream_id)
      raise NotImplementedError, "implement #init_state(stream_id) => Object in #{self.class}"
    end

    def handle_command(command)
      # TODO: this might raise an exception from a worker
      # Think what to do with invalid commands here
      raise "invalid command #{command.inspect} #{command.errors.inspect}" unless command.valid?
      logger.info "#{self.class} Handling #{command.type}"
      state = load(command.stream_id)
      events = decide(state, command)
      state = evolve(state, events)
      transaction do
        events = save(state, command, events)
        # handle sync reactors here
        # commands = react_sync(state, events)
        # Schedule a system command to handle this batch of events in the background
        # schedule_batch(command, commands)
        # schedule_commands(commands)
      end
      [ state, events ]
    end

    # Reactor interface
    def handle_events(events, &map_commands)
      commands = react(events)
      commands = commands.map(&map_commands) if map_commands
      commands
    end

    private

    attr_reader :logger

    def load(stream_id)
      state = init_state(stream_id)
      events = backend.read_event_stream(stream_id)
      @seq = events.last&.seq || 0
      evolve(state, events)
    end

    # Register a first sync block to append new events to backend
    sync do |_state, command, events|
      backend.append_to_stream(command.stream_id, [command, *events])
    end

    def save(state, command, events)
      # Update :seq for each event based on seq
      # TODO: we do the same in Aggregate#save. DRY this up
      events = [command, *events].map do |event|
        @seq += 1
        event.with(seq: @seq)
      end
      backend.transaction do
        run_sync_blocks(state, events[0], events[1..-1])
      end
      events
    end

    # def schedule_batch(command, commands)
    #   schedule_commands([command.follow(ProcessBatch), *commands])
    # end
    #
    # def schedule_commands(commands)
    #   backend.schedule_commands(commands)
    # end

    def transaction(&)
      backend.transaction(&)
    end
  end
end
