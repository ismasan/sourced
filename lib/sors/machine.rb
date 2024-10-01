# frozen_string_literal: true

require 'sors/router'
require 'sors/reactor'
require 'sors/message'

module Sors
  class Machine
    class << self
      def handled_commands
        @handled_commands ||= []
      end

      def handled_events
        @handled_events ||= []
      end

      def persister
        @persister ||= ->(state, command, events) { logger.info("Persisting #{state}, #{command}, #{events}") }
      end

      def loader
        @loader ||= ->(command) { raise NotImplementedError.new("No loader defined for #{self}") }
      end

      def reactor
        @reactor ||= Class.new(Reactor)
      end

      def load(loader = nil, &block)
        @loader = loader || block
      end

      def decide(cmd_type, &block)
        handled_commands << cmd_type
        define_method(message_method_name(cmd_type.name), &block)
      end

      def evolve(event_type, &block)
        handled_events << event_type
        define_method(message_method_name(event_type.name), &block)
      end

      def persist(&block)
        @persister = block
      end

      def react(event_type, &block)
        reactor.react(event_type, &block)
      end

      def message_method_name(name)
        "__handle_#{name.split('::').map(&:downcase).join('_')}"
      end
    end

    attr_reader :backend

    def initialize(logger: Sors.config.logger, backend: Sors.config.backend)
      @logger = logger
      @backend = backend
    end

    def inspect
      %(<#{self.class}#{object_id} backend:#{backend.inspect}>)
    end

    def ==(other)
      other.is_a?(self.class) && other.backend == backend
    end

    def handled_commands = self.class.handled_commands
    def handled_events = self.class.handled_events
    def reactor = self.class.reactor

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

    def decide(state, command)
      events = send(self.class.message_method_name(command.class.name), state, command)
      [ events ].flatten.compact
    end

    def evolve(state, events)
      events.each do |event|
        method_name = self.class.message_method_name(event.class.name)
        send(method_name, state, event) if respond_to?(method_name)
      end

      state
    end

    def react(_state, events)
      self.class.reactor.call(events)
    end

    def persist(state, command, events)
      self.class.persister.call(state, command, events)
    end

    ProcessBatch = Message.define('machine.batch.process')

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
