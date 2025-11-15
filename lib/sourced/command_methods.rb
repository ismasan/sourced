# frozen_string_literal: true

module Sourced
  # Provides command invocation methods for actors.
  #
  # CommandMethods automatically generates instance methods from command definitions,
  # allowing you to invoke commands in two ways:
  #
  # 1. **In-memory version** (e.g., `actor.start(name: 'Joe')`)
  #    - Validates the command and executes the decision handler
  #    - Returns a tuple of [cmd, new_events]
  #    - Does NOT persist events to backend
  #
  # 2. **Durable version** (e.g., `actor.start!(name: 'Joe')`)
  #    - Same as in-memory, but also appends events to backend
  #    - Raises {FailedToAppendMessagesError} if backend fails
  #
  # ## Usage
  #
  # Include the module in an Actor and define commands normally:
  #
  #     class MyActor < Sourced::Actor
  #       include Sourced::CommandMethods
  #
  #       command :create_item, name: String do |state, cmd|
  #         event :item_created, cmd.payload
  #       end
  #     end
  #
  #     actor = MyActor.new(id: 'actor-123')
  #     cmd, events = actor.create_item(name: 'Widget')  # In-memory
  #     cmd, events = actor.create_item!(name: 'Widget') # Persists to backend
  #
  # ## Method Naming
  #
  # Methods are created based on your command name:
  # - Symbol commands like `:create_item` → `create_item` and `create_item!` methods
  # - Class commands like `CreateItem` → `create_item` and `create_item!` methods
  #
  module CommandMethods
    # Raised when the durable command (bang method) fails to append events to the backend.
    #
    # @see #__issue_command
    class FailedToAppendMessagesError < StandardError
      def initialize(cmd, events)
        super <<~MSG
        Failed to append events to backend.
        Command is #{cmd.inspect}
        Events are #{events.inspect}
        MSG
      end
    end

    def self.included(base)
      base.send :extend, ClassMethods
    end

    # Issues a command without persisting to the backend.
    #
    # This is the core logic used by the generated command methods. It validates
    # the command and executes the decision handler if valid.
    #
    # @private
    # @param cmd_class [Class] A subclass of {Sourced::Message} representing the command
    # @param payload [Hash] The command payload data
    # @return [Array<(Sourced::Message, Array<Sourced::Message>)>] A tuple of [command, events]
    #   where command is the validated command object and events are the produced events
    #   (empty array if command was invalid)
    #
    # @example
    #   cmd, events = __issue_command(MyActor::CreateItem, name: 'Widget')
    #   puts cmd.valid?     # => true
    #   puts events.length  # => 1
    private def __issue_command(cmd_class, payload = {})
      cmd = cmd_class.new(stream_id: id, payload:)
      return [cmd, React::EMPTY_ARRAY] unless cmd.valid?

      [cmd, decide(cmd)]
    end

    # @private
    # Class methods that hook into the command definition and create instance methods.
    module ClassMethods
      # Hooks into the command definition to automatically generate instance methods.
      #
      # This method is called automatically when you define a command in your actor.
      # It creates both in-memory and durable (bang) versions of the command method.
      #
      # @see Sourced::Actor.command
      def command(*args, &block)
        super.tap do |_|
          case args
          in [Symbol => cmd_name, *_]
            klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
            cmd_class = const_get(klass_name)
            __command_methods_define(cmd_name, cmd_class)
          in [Class => cmd_class] if cmd_class < Sourced::Message
            cmd_name = Sourced::Types::ModuleToMethodName.parse(cmd_class.name.split('::').last)
            __command_methods_define(cmd_name, cmd_class)
          end
        end
      end

      # Creates in-memory and durable command invocation methods.
      #
      # Defines two methods on the actor instance:
      # - `method_name(**payload)` - In-memory version, validates and decides without persisting
      # - `method_name!(**payload)` - Durable version, also appends events to backend
      #
      # @private
      # @param cmd_name [Symbol] The command method name (e.g., :create_item)
      # @param cmd_class [Class] The command message class
      #
      # @example Generated methods
      #   class MyActor < Sourced::Actor
      #     include Sourced::CommandMethods
      #     command :create_item, name: String do |state, cmd|
      #       event :item_created, cmd.payload
      #     end
      #   end
      #
      #   actor = MyActor.new(id: 'a1')
      #
      #   # In-memory version
      #   cmd, events = actor.create_item(name: 'Widget')
      #   # => Returns [cmd, events] without touching backend
      #   # => If invalid: [invalid_cmd, []]
      #
      #   # Durable version
      #   cmd, events = actor.create_item!(name: 'Widget')
      #   # => Returns [cmd, events] if valid and appended successfully
      #   # => Raises FailedToAppendMessagesError if backend fails
      #   # => If invalid: [invalid_cmd, []]
      private def __command_methods_define(cmd_name, cmd_class)
        define_method(cmd_name) do |**payload|
          __issue_command(cmd_class, payload)
        end

        define_method("#{cmd_name}!") do |**payload|
          cmd_events = __issue_command(cmd_class, payload)
          return cmd_events unless cmd_events.first.valid?

          success = Sourced.config.backend.append_to_stream(id, cmd_events.last)
          raise FailedToAppendMessagesError.new(*cmd_events) unless success

          cmd_events
        end
      end
    end
  end
end
