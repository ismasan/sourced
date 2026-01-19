# frozen_string_literal: true

module Sourced
  # This mixin provides a .react macro to register
  # message handlers for a class
  # These message handlers are "reactions", ie. they react to
  # messages by producing new commands which will initiate new Decider flows.
  # More here: https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/#3-react
  #
  # Example:
  #
  #  class Saga
  #    include Sourced::React
  #
  #    # Host class must implement a #state method
  #    # which will be passed to reaction handlers
  #    attr_reader :state
  #
  #    def initialize(id:)
  #      @state = { id: }
  #    end
  #
  #    # React to an event and return a new command.
  #    # This command will be scheduled for processing by a Decider.
  #    # Using Sourced::Event#follow copies over metadata from the event
  #    # including causation and correlation IDs.
  #    reaction SomethingHappened do |state, event|
  #      event.follow(DoSomethingElse, field1: 'value1')
  #    end
  #  end
  #
  #  saga = Saga.new(id: '123')
  #  commands = saga.react([something_happened])
  #
  module React
    PREFIX = 'reaction'
    EMPTY_ARRAY = [].freeze

    def self.included(base)
      super
      base.extend ClassMethods
    end

    # @param events [Array<Sourced::Event>]
    # @return [Array<Sourced::Command>]
    def react(events)
      __handling_reactions(Array(events)) do |event|
        method_name = Sourced.message_method_name(React::PREFIX, event.class.to_s)
        if respond_to?(method_name)
          Array(send(method_name, state, event)).compact
        else
          EMPTY_ARRAY
        end
      end
    end

    # TODO: O(1) lookup
    def reacts_to?(message)
      self.class.handled_messages_for_react.include?(message.class)
    end

    private

    def __handling_reactions(events, &)
      @__stream_dispatchers = []
      events.each do |event|
        @__event_for_reaction = event
        yield event
      end
      cmds = @__stream_dispatchers.map(&:message)
      @__stream_dispatchers.clear
      cmds
    end

    class Dispatcher
      attr_reader :message

      def initialize(msg)
        @message = msg
      end

      def inspect = %(<#{self.class} #{@message}>)

      def to(stream_id)
        @message = @message.to(stream_id)
        self
      end

      def at(datetime)
        @message = @message.at(datetime)
        self
      end

      def with_metadata(attrs = {})
        @message = @message.with_metadata(attrs)
        self
      end
    end

    def dispatch(command_class, payload = {})
      command_class = self.class[command_class] if command_class.is_a?(Symbol)
      cmd = @__event_for_reaction
            .follow(command_class, payload)
            .with_metadata(producer: self.class.consumer_info.group_id)

      dispatcher = Dispatcher.new(cmd)
      @__stream_dispatchers << dispatcher
      dispatcher
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_messages_for_react.each do |evt_type|
          subclass.handled_messages_for_react << evt_type
        end
      end

      # Override this with extend Sourced::Consumer
      def consumer_info
        Sourced::Consumer::ConsumerInfo.new(group_id: name)
      end

      def handled_messages_for_react
        @handled_messages_for_react ||= []
      end

      # Define a reaction to an event
      # @example
      #   reaction SomethingHappened do |state, event|
      #     stream = stream_for(event)
      #     # stream = stream_for("new-stream-id")
      #     stream.command DoSomethingElse
      #   end
      #
      # The host class is expected to define a #state method
      # These handlers will load the decider's state from past events, and yield the state and the event to the block.
      # @example
      #   reaction SomethingHappened do |state, event|
      #     if state[:count] % 3 == 0
      #       steam_for(event).command DoSomething
      #     end
      #   end
      #
      # If no event class given, the handler is registered for all events 
      # set to evolve in .handled_messaged_for_evolve, unless 
      # specific reactions have already been registered for them
      # The host class is expected to support .handled_messaged_for_evolve
      # see Evolve mixin
      # @example
      #   reaction do |state, event|
      #     LOGGER.info state
      #   end
      #
      # @overload reaction do |state, event|
      # @overload reaction(event_symbol) do |state, event|
      #   @param event_symbol [Symbol] Symbolised message name
      # @overload reaction(event_class) do |state, event|
      #   @param event_class [Class] Must be subclass of Sourced::Message
      # @overload reaction(*events) do |state, event|
      #   @param *events [Array<Object>] List of event classes or symbols
      # @return [void]
      def reaction(*args, &block)
        case args
        in []
          handled_messages_for_evolve.each do |e|
            method_name = Sourced.message_method_name(React::PREFIX, e.to_s)
            if !instance_methods.include?(method_name.to_sym)
              reaction e, &block
            end
          end

        in [Symbol => message_symbol]
          message_class = __resolve_message_class(message_symbol)
          reaction(message_class, &block)

        in [Class => message_class] if message_class < Sourced::Message
          __validate_message_for_reaction!(message_class)
          unless message_class.is_a?(Class) && message_class < Sourced::Message
            raise ArgumentError,
                  "Invalid argument #{message_class.inspect} for #{self}.reaction"
          end

          self.handled_messages_for_react << message_class
          define_method(Sourced.message_method_name(React::PREFIX, message_class.to_s), &block) if block_given?
        in Array => args if args.none?(&:nil?)
          args.each do |k|
            reaction k, &block
          end
        else
          raise ArgumentError,
                "Invalid arguments #{args.inspect} for #{self}.reaction"
        end
      end

      # Run this hook before registering a reaction
      # Actor can override this to make sure that the same message is not
      # also handled as a command
      def __validate_message_for_reaction!(event_class)
        # no-op.
      end

      private

      def __resolve_message_class(message_symbol)
        raise ArgumentError, "#{self} doesn't support resolving #{message_symbol.inspect} into a message class"
      end
    end
  end
end
