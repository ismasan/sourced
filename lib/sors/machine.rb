# frozen_string_literal: true

require 'sors/router'
require 'sors/message'

module Sors
  class Machine
    include Decide
    include Evolve
    include React
    include ReactSync

    ProcessBatch = Message.define('machine.batch.process')

    attr_reader :backend

    class << self
      def handled_events = self.handled_events_for_react

      def handle_command(command)
        new.handle_command(command)
      end

      def handle_events(events)
        new.handle_events(events)
      end
    end

    def initialize(logger: Sors.config.logger, backend: Sors.config.backend)
      @logger = logger
      @backend = backend
    end

    def inspect
      %(<#{self.class}:#{object_id} backend:#{backend.inspect}>)
    end

    def ==(other)
      other.is_a?(self.class) && other.backend == backend
    end

    def handle_command(command)
      logger.info "Handling #{command.type}"
      state = load(command)
      events = decide(state, command)
      state = evolve(state, events)
      transaction do
        save(state, command, events)
        # handle sync reactors here
        commands = react_sync(state, events)
        # Schedule a system command to handle this batch of events in the background
        schedule_batch(command, commands)
        # schedule_commands(commands)
      end
      [ state, events ]
    end

    # Reactor interface
    def handle_events(events, &map_commands)
      state = load(events.first)
      commands = react(state, events)
      commands = commands.map(&map_commands) if map_commands
      schedule_commands(commands)
    end

    private

    attr_reader :logger

    def load(command)
      raise NotImplementedError, 'implement a #load(command) => state'
    end

    def save(state, command, events)
      raise NotImplementedError, 'implement a #save(state, command, events) method'
    end

    def schedule_batch(command, commands)
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
