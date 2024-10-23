# frozen_string_literal: true

require 'sors/router'
require 'sors/message'

module Sors
  class Aggregate
    include Decide
    include Evolve
    include React

    class CommandSpec
      attr_reader :payload_spec, :run_spec

      def initialize(&block)
        @payload_spec = -> {}
        @run_spec = -> {}
        instance_eval(&block)
        freeze
      end

      def payload(&block)
        @payload_spec = block
      end

      def run(&block)
        @run_spec = block
      end
    end

    class << self
      def handled_events = self.handled_events_for_react

      # The Decider interface
      def handle_command(cmd)
        load(cmd.stream_id).handle_command(cmd)
      end

      def handle_events(events)
        load(events.first.stream_id).handle_events(events)
      end

      def create
        new(SecureRandom.uuid)
      end

      def load(id)
        new(id).load
      end

      def command(cmd_name, message_type = nil, &block)
        segments = name.split('::').map(&:downcase)
        spec = CommandSpec.new(&block)
        message_type ||= [*segments, cmd_name].join('.')
        klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
        cmd_class = Message.define(message_type, &spec.payload_spec)
        const_set(klass_name, cmd_class)
        decide cmd_class, &spec.run_spec
        define_method(cmd_name) do |**args|
          command cmd_class, args
        end
      end
    end

    attr_reader :id, :logger, :seq

    def initialize(id)
      @id = id
      # TODO: per-stream sequence
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
        save(command, events)
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
      evolve(events)
      self
    end

    def save(command, events)
      backend.append_events([command, *events])
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
