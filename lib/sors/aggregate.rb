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
      # @param cmd [Command]
      def handle_command(cmd)
        load(cmd.stream_id).handle_command(cmd)
      end

      # Create a new Aggregate instance
      #
      # @param stream_id [String] the stream id
      # @return [Aggregate]
      def build(stream_id = SecureRandom.uuid)
        new(stream_id)
      end

      # Load an Aggregate from event history
      #
      # @param stream_id [String] the stream id
      # @return [Aggregate]
      def load(stream_id, upto: nil)
        new(stream_id).load(upto:)
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
      @seq = 0
      @logger = Sors.config.logger
      @backend = Sors.config.backend
      setup(id)
    end

    private def setup(id)
    end

    def ==(other)
      other.is_a?(self.class) && id == other.id && seq == other.seq
    end

    def handle_command(command)
      # TODO: this might raise an exception from a worker
      # Think what to do with invalid commands here
      raise "invalid command #{command.inspect} #{command.errors.inspect}" unless command.valid?
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

    # TODO: idempotent event and command handling
    # Reactor interface
    def handle_events(events, &map_commands)
      commands = react(events)
      commands = commands.map(&map_commands) if map_commands
      schedule_commands(commands)
    end

    def load(after: nil, upto: nil)
      events = backend.read_event_stream(id, after:, upto:)
      if events.any?
        @seq = events.last.seq 
        evolve(events)
      end
      self
    end

    def catch_up
      load(after: seq)
    end

    def events
      backend.read_event_stream(id, upto: seq)
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
