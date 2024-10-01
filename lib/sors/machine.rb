# frozen_string_literal: true

require 'sors/router'
require 'sors/reactor'
require 'sors/message'

module Sors
  class Machine
    include Decide
    include Evolve
    include React

    ProcessBatch = Message.define('machine.batch.process')

    class << self
      def persister
        @persister ||= ->(state, command, events) { logger.info("Persisting #{state}, #{command}, #{events}") }
      end

      def loader
        @loader ||= ->(command) { raise NotImplementedError.new("No loader defined for #{self}") }
      end

      def load(loader = nil, &block)
        @loader = loader || block
      end

      def persist(&block)
        @persister = block
      end
    end

    attr_reader :backend

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

    def handle(command)
      logger.info "Handling #{command.type}"
      state = load_state(command)
      events = decide(state, command)
      state = evolve(state, events)
      transaction do
        persist(state, command, events)
        # TODO: handle sync reactors here
        # commands = react(state, events)
        # Schedule a system command to handle this batch of events in the background
        schedule_batch(command)
        # schedule_commands(commands)
      end
      [ state, events ]
    end

    private

    attr_reader :logger

    def load_state(command)
      self.class.loader.call(command)
    end

    def persist(state, command, events)
      self.class.persister.call(state, command, events)
    end

    def schedule_batch(command)
      schedule_commands([command.follow(ProcessBatch)])
    end

    def schedule_commands(commands)
      backend.schedule_commands(commands)
    end

    def transaction(&)
      backend.transaction(&)
    end
  end
end
