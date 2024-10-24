# frozen_string_literal: true

require 'sors/router'
require 'sors/message'

module Sors
  class Aggregate
    include Decide
    include Evolve
    include React

    class << self
      def handled_events = self.handled_events_for_react

      # The Decider interface
      # @param cmd [Command]
      def handle_command(cmd)
        load(cmd.stream_id).handle_command(cmd)
      end

      # The Reactor interface
      # @param events [Array<Event>]
      def handle_events(events)
        load(events.first.stream_id).handle_events(events)
      end

      def create
        new(SecureRandom.uuid)
      end

      # @param stream_id [String]
      # @return [Aggregate]
      def load(stream_id)
        new(stream_id).load
      end

      # Define a command class, register a command handler
      # and define a method to send the command
      # Example:
      #   command :add_item, name: String do |cmd|
      #     cmd.follow(ItemAdded, item_id: SecureRandom.uuid, name: cmd.payload.name)
      #   end
      #
      # # The exmaple above will define a command class `AddItem` in the current namespace:
      # AddItem = Message.define('namespace.add_item', payload_schema: { name: String })
      #
      # # It will also register the command handler:
      # decide AddItem do |cmd|
      #   cmd.follow(ItemAdded, item_id: SecureRandom.uuid, name: cmd.payload.name)
      # end
      #
      # # And an :add_item method to send the command:
      # def add_item(name:)
      #   issue_command AddItem, name:
      # end
      #
      # This method can be used on Aggregate instances:
      #   aggregate.add_item(name: 'Buy milk')
      #
      # Payload schema is a Plumb Hash schema.
      # See: https://github.com/ismasan/plumb#typeshash
      #
      # The helper method will instantiate an instance of the command class
      # and validate its attributes with #valid?
      # Only valid commands will be issued to the handler.
      # The method returns the command instance. If #valid? is false, then the command was not issued.
      # Example:
      #   cmd = aggregate.add_item(name: 10)
      #   cmd.valid? # => false
      #   cmd.errors # => { name: 'must be a String' }
      #
      # @param cmd_name [Symbol] example: :add_item
      # @param payload_schema [Hash] A Plumb Hash schema. example: { name: String }
      # @param block [Proc] The command handling code
      # @return [Message] the command instance, which can be #valid? or not
      def command(cmd_name, payload_schema = {}, &block)
        segments = name.split('::').map(&:downcase)
        message_type ||= [*segments, cmd_name].join('.')
        klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
        cmd_class = Message.define(message_type, payload_schema:)
        const_set(klass_name, cmd_class)
        decide cmd_class, &block
        define_method(cmd_name) do |**payload|
          issue_command cmd_class, payload
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
      raise "invalid command #{command.inspect}" unless command.valid?
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

    def issue_command(klass, payload = {})
      cmd = klass.new(stream_id: id, payload:)
      return cmd unless cmd.valid?

      handle_command cmd
      cmd
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
