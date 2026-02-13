# frozen_string_literal: true

module Sourced
  class Actor
    include Evolve
    include React
    include Sync
    extend Consumer

    PREFIX = 'decide'

    UndefinedMessageError = Class.new(KeyError)
    Error = Class.new(StandardError)

    class DualMessageRegistrationError < Error
      def initialize(msg_type, handled_by)
        msg = if handled_by == :reaction
          <<~MSG
          Message #{msg_type} is already registered to be handled by .reaction(#{msg_type}).
          Sourced::Actor classes can only handle the same message type either as a reaction, or a command.
          MSG
        else
          <<~MSG
          Message #{msg_type} is already registered to be handled by .command(#{msg_type}).
          Sourced::Actor classes can only handle the same message type either as a reaction, or a command.
          MSG
        end
        super msg
      end
    end

    class DifferentStreamError < Error
      def initialize(actor, event)
        super <<~MSG
        Actor instance #{actor.inspect} was initialized with id = '#{actor.id}',
        but it was evolved with an event for stream_id '#{event.stream_id}',

        The event is #{event.inspect}
        MSG
      end
    end

    class SmallerSequenceError < Error
      def initialize(actor, event)
        super <<~MSG
        Actor instance #{actor.inspect} is currently at sequence number #{actor.seq},
        but it was evolved with an event at sequence #{event.seq}.

        The event is #{event.inspect}
        MSG
      end
    end

    # An Actor class has its own Command and Event
    # subclasses that are used to define inine commands and events.
    # These classes serve as message registry for the Actor's inline messages.
    class Command < Sourced::Command; end
    class Event < Sourced::Event; end

    BLANK_HISTORY = [].freeze

    class << self
      def inherited(subclass)
        super
        subclass.const_set(:Command, Class.new(const_get(:Command)))
        subclass.const_set(:Event, Class.new(const_get(:Event)))
        handled_commands.each do |cmd_type|
          subclass.handled_commands << cmd_type
        end
      end

      # Access a Actor's Command or Event classes by name (e.g. :some_command or :some_event)
      # @param message_name [Symbol]
      # @return [Class]
      # @raise [ArgumentError] if the message is not defined
      def resolve_message_class(message_name)
        message_type = __message_type(message_name)
        msg_class = self::Event.registry[message_type] || self::Command.registry[message_type]

        raise UndefinedMessageError, "Message not found: #{message_name}" unless msg_class
        msg_class
      end

      alias [] resolve_message_class

      # Interface expected by React::StreamDispatcher
      # so that this works in reaction blocks
      # @example
      #   stream = stream_for(SomeActor)
      #   stream.command :do_something
      #
      # @return [String]
      def stream_id
        Sourced.new_stream_id
      end

      def handled_commands
        @handled_commands ||= []
      end

      # Register as a Reactor
      # @return [Array<Sourced::Message>]
      def handled_messages = self.handled_commands + self.handled_messages_for_react

      # Define a command class, register a command handler
      # and define a method to send the command
      # Example:
      #   command :add_item, name: String do |state, cmd|
      #     event(ItemAdded, item_id: SecureRandom.uuid, name: cmd.payload.name)
      #   end
      #
      # # The exmaple above will define a command class `AddItem` in the current namespace:
      # AddItem = Message.define('namespace.add_item', payload_schema: { name: String })
      #
      # Payload schema is a Plumb Hash schema.
      # See: https://github.com/ismasan/plumb#typeshash
      #
      def command(*args, &block)
        raise ArgumentError, 'command block expects signature (state, command)' unless block.arity == 2

        case args
          in [Symbol => cmd_name, Hash => payload_schema]
            __register_named_command_handler(cmd_name, payload_schema, &block)
          in [Symbol => cmd_name]
            __register_named_command_handler(cmd_name, &block)
          in [Class => cmd_type] if cmd_type < Sourced::Message
            __register_class_command_handler(cmd_type, &block)
        else
          raise ArgumentError, "Invalid arguments for #{self}.command"
        end
      end

      # Support defining event handlers with a symbol and a payload schema
      # Or a class.
      #
      # @example
      #
      #   event SomethingHappened, field1: String do |state, event|
      #     state[:status] = 'done'
      #   end
      #
      #   event :something_happened, field1: String do |state, event|
      #     state[:status] = 'done'
      #   end
      #
      def event(*args, &block)
        case args
          in [Symbol => event_name, Hash => payload_schema]
            __register_named_event_handler(event_name, payload_schema).tap do |event_class|
              super(event_class, &block)
            end
          in [Symbol => event_name]
            __register_named_event_handler(event_name).tap do |event_class|
              super(event_class, &block)
            end
          in [Class => foo]
            super
          else
            raise ArgumentError, "event expects a Symbol or Event class. Got: #{args.inspect}"
        end
      end

      # The Reactor interface
      # Message can be:
      # - A command, present in .handled_commands
      # - A reaction, present in .handled_messages_for_react
      # @param message [Sourced::Message]
      # @option history [Enumerable<Sourced::Message>] past messages in the stream
      # @return [Sourced::Actions]
      def handle(message, history: [], replaying: false)
        instance = new(id: identity_from(message))
        instance.handle(message, history:, replaying:)
      end

      # Batch processing for actors. Per-message iteration internally.
      # Replay messages return OK immediately. Live messages create instance and handle.
      # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
      # @param history [Array<Message>] full stream history
      # @return [Array<[actions, source_message]>] action pairs
      def handle_batch(batch, history: BLANK_HISTORY)
        batch.map do |message, replaying|
          if replaying
            [Actions::OK, message]
          else
            instance = new(id: identity_from(message))
            actions = instance.handle(message, history:)
            [actions, message]
          end
        end
      end

      def __message_type(msg_name)
        [__message_type_prefix, msg_name].join('.').downcase
      end

      # Override this in subclasses 
      # to make an actor take it's @id from an arbitrary 
      # field in the message
      # TODO: the danger here is that not all messages might 
      # support this arbitrary field.
      # it would be nice if we could statically
      # analyse messages handled by the Actor, and raise an error
      # if not all of them support the specified field
      #
      # @param message [Sourced::Message]
      # @return [Object]
      def identity_from(message) = message.stream_id

      private

      def __register_named_command_handler(cmd_name, payload_schema = nil, &block)
        cmd_class = self::Command.define(__message_type(cmd_name), payload_schema:)
        klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
        const_set(klass_name, cmd_class)
        __register_class_command_handler(cmd_class, &block)
      end

      def __register_class_command_handler(cmd_type, &block)
        raise DualMessageRegistrationError.new(cmd_type, :reaction) if handled_messages_for_react.include?(cmd_type)

        handled_commands << cmd_type
        define_method(Sourced.message_method_name(PREFIX, cmd_type.name), &block)
      end

      def __register_named_event_handler(event_name, payload_schema = nil)
        klass_name = event_name.to_s.split('_').map(&:capitalize).join
        event_class = self::Event.define(__message_type(event_name), payload_schema:)
        const_set(klass_name, event_class)
      end

      # Override this method defined in React mixin.
      # Given a symbol for a message type, resolve the message class
      # @return [Class<Sourced::Message>, nil]
      def __resolve_message_class(message_symbol)
        self::Event.registry[__message_type(message_symbol)]
      end

      # Override the default namespace for commands and events
      # defined inline
      # @example
      #
      #   def message_namespace = 'my_app.messages.'
      #
      # @return [String]
      def message_namespace
        Types::ModuleToMessageType.parse(name.to_s)
      end

      def __message_type_prefix
        @__message_type_prefix ||= message_namespace
      end

      # Override the no-op hook in Sourced::React
      # We want to make sure that a message handled as a command
      # cannot also be registered as a reaction.
      # These are the command/event semantics that Actor adds on top
      # of the underlying messaging infrastructure.
      def __validate_message_for_reaction!(event_class)
        raise DualMessageRegistrationError.new(event_class, :command) if handled_commands.include?(event_class)
      end
    end

    # Instance methods

    attr_reader :id, :seq, :uncommitted_events

    def initialize(id: Sourced.new_stream_id, logger: Sourced.config.logger)
      @id = id
      @seq = 0
      @uncommitted_events = []
      @logger = logger
      @__current_command = Sourced::Command.new(stream_id: id)
    end

    def inspect
      %(<#{self.class} id:#{id} seq:#{seq}>)
    end

    def handle(message, history:, replaying: false)
      return Actions::OK if replaying

      evolve(history) 
      if handles_command?(message)
        events = decide(message)
        actions = [Actions::AppendAfter.new(id, events)]
        actions + sync_actions_with(command: message, events:, state:)
      elsif reacts_to?(message)
        Actions.build_for(react(message))
      else
        Actions::OK
      end
    end

    # Does this actor handle this message as a command?
    # TODO: ATM I'm just doing .handled_commands.include?
    # it would be more efficient to have an O(1) lookup
    def handles_command?(message)
      self.class.handled_commands.include?(message.class)
    end

    # Route a command to its defined command handler, and run it.
    # @param command [Sourced::Command]
    # @return [Array<Any, Array<Sourced::Event>]
    def decide(command)
      command = __set_current_command(command)
      send(Sourced.message_method_name(PREFIX, command.class.name), state, command)
      @__current_command = nil
      @uncommitted_events.slice!(0..)
    end

    # Apply an event from within a command handler
    # @example
    #
    #  command DoSomething do |state, cmd|
    #    event SomethingHappened, field1: 'foo', field2: 'bar'
    #  end
    #
    # Or, with symbol pointing to an event class defined with .event
    #   command DoSomething do |state, cmd|
    #     event :something_happened, field1: 'foo', field2: 'bar'
    #   end
    #
    # @param event_name [Symbol, Class] the event name or class
    # @param payload [Hash] the event payload
    # @return [Any] the
    def event(event_name, payload = {})
      return apply(event_name, payload) unless event_name.is_a?(Symbol)

      event_class = Event.registry[self.class.__message_type(event_name)]
      raise ArgumentError, "Event not found: #{event_name}" unless event_class

      apply(event_class, payload)
    end

    private

    attr_reader :__current_command, :logger

    # Override Evolve#__update_on_evolve
    def __update_on_evolve(event)
      raise DifferentStreamError.new(self, event) if id != event.stream_id
      raise SmallerSequenceError.new(self, event) if seq >= event.seq

      @seq = event.seq
    end

    # TODO: in the new arch, commands 
    # already exist in the event stream
    # so we don't append them again as part of uncommitted_events
    # and we don't increment @seq
    # However, when handling commands asynchronously,
    # we DO also want to append the command with the produced events
    # Think about this later.
    def __set_current_command(command)
      @__current_command = command
    end

    def __next_sequence
      @seq + 1
    end

    # Instantiate an event class and apply it to the state
    # by running registered evolve blocks.
    # Also store the event in the uncommitted events list,
    # and keep track of the sequence number.
    # To be used inside a .decide block.
    # @example
    #
    #  apply SomeEvent, field1: 'foo', field2: 'bar'
    #
    # @param event_class [Sourced::Event]
    # @param payload [Hash] the event payload
    # @return [Any] the new state
    def apply(event_class, payload = {})
      evt = __current_command.follow_with_attributes(
        event_class, 
        attrs: { seq: __next_sequence }, 
        # TODO: the infra sets this now
        # metadata: { producer: self.class.consumer_info.group_id },
        payload:
      )
      uncommitted_events << evt
      evolve([evt])
    end
  end
end
