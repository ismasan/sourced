# frozen_string_literal: true

module Sourced
  class Decider
    extend Consumer
    include Evolve
    include React
    include Sync

    PREFIX = 'decide'

    class << self
      def inherited(subclass)
        super
        handled_commands.each do |cmd_type|
          subclass.handled_commands << cmd_type
        end
      end

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

      # Load a Decider from event history
      #
      # @param stream_id [String] the stream id
      # @return [Decider]
      def load(stream_id, upto: nil)
        new(stream_id).load(upto:)
      end

      def handled_commands
        @handled_commands ||= []
      end

      def decide(cmd_type, &block)
        handled_commands << cmd_type
        define_method(Sourced.message_method_name(PREFIX, cmd_type.name), &block)
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
      # # Optionally you can pass an explicit command type string:
      #   command :add_item, 'todos.items.add', name: String do |cmd|
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
      # This method can be used on Decider instances:
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
      def command(*args, &block)
        raise ArgumentError, 'command block expects signature (state, command)' unless block.arity == 2

        message_type = nil
        cmd_name = nil
        payload_schema = {}
        segments = name.split('::').map(&:downcase)

        case args
          in [cmd_name]
            message_type = [*segments, cmd_name].join('.')
          in [cmd_name, Hash => payload_schema]
            message_type = [*segments, cmd_name].join('.')
          in [cmd_name, String => message_type, Hash => payload_schema]
          in [cmd_name, String => message_type]
        else
          raise ArgumentError, "Invalid arguments for #{self}.command"
        end

        klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
        cmd_class = Message.define(message_type, payload_schema:)
        const_set(klass_name, cmd_class)
        decide cmd_class, &block
        define_method(cmd_name) do |**payload|
          issue_command cmd_class, payload
        end
      end
    end

    attr_reader :id, :seq, :state, :uncommitted_events

    def initialize(id = SecureRandom.uuid, backend: Sourced.config.backend, logger: Sourced.config.logger)
      @id = id
      @backend = backend
      @logger = logger
      @seq = 0
      @state = init_state(id)
      @uncommitted_events = []
      @__current_command = nil
    end

    def inspect
      %(<#{self.class} id:#{id} seq:#{seq}>)
    end

    def ==(other)
      other.is_a?(self.class) && id == other.id && seq == other.seq
    end

    def init_state(id)
      nil
    end

    def load(after: nil, upto: nil)
      events = backend.read_event_stream(id, after:, upto:)
      if events.any?
        @seq = events.last.seq 
        evolve(state, events)
      end
      self
    end

    def catch_up
      seq_was = seq
      load(after: seq_was)
      [seq_was, seq]
    end

    def events(upto: seq)
      backend.read_event_stream(id, upto:)
    end

    def decide(command)
      command = __set_current_command(command)
      send(Sourced.message_method_name(PREFIX, command.class.name), state, command)
      @__current_command = nil
      [state, uncommitted_events]
    end

    def apply(event_class, payload = {})
      evt = __current_command.follow_with_seq(event_class, __next_sequence, payload)
      uncommitted_events << evt
      evolve(state, [evt])
    end

    def commit(&)
      output_events = uncommitted_events.slice(0..-1)
      yield output_events
      @uncommitted_events = []
      output_events
    end

    # Register a first sync block to append new events to backend
    sync do |_state, command, events|
      messages = [command, *events]
      backend.append_to_stream(id, messages) if messages.any?
    end

    def handle_command(command)
      # TODO: this might raise an exception from a worker
      # Think what to do with invalid commands here
      raise "invalid command #{command.inspect} #{command.errors.inspect}" unless command.valid?
      logger.info "#{self.inspect} Handling #{command.type}"
      decide(command)
      save
    end

    # TODO: idempotent event and command handling
    # Reactor interface
    # Handle events, return new commands
    # Workers will handle route these commands
    # to their target Deciders
    def handle_events(events)
      react(events)
    end

    private

    attr_reader :backend, :logger, :__current_command

    def save
      events = commit do |messages|
        backend.transaction do
          run_sync_blocks(state, messages[0], messages[1..-1])
        end
      end
      [ self, events ]
    end

    def __set_current_command(command)
      command.with(seq: __next_sequence).tap do |cmd|
        uncommitted_events << cmd
        @__current_command = cmd
      end
    end

    def __next_sequence
      @seq += 1
    end

    def issue_command(klass, payload = {})
      cmd = klass.new(stream_id: id, payload:)
      return cmd unless cmd.valid?

      handle_command(cmd)
      cmd
    end
  end
end
