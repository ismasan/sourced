# frozen_string_literal: true

require 'set'

module Sourced
  # React mixin for reactors.
  # Supports the same dispatch-based reaction DSL as Sourced::React,
  # adapted to Sourced's stream-less messages.
  module React
    PREFIX = 'sourced_reaction'
    EMPTY_ARRAY = [].freeze

    def self.included(base)
      super
      base.extend ClassMethods
    end

    # Run reaction handlers for one or more messages.
    # Supports both explicit message returns and dispatch(...) calls.
    def react(messages)
      __handling_reactions(Array(messages)) do |message|
        method_name = Sourced.message_method_name(PREFIX, message.class.to_s)
        if respond_to?(method_name)
          Array(send(method_name, state, message)).compact
        else
          EMPTY_ARRAY
        end
      end
    end

    def reacts_to?(message)
      self.class.handled_messages_for_react.include?(message.class)
    end

    private

    def __handling_reactions(messages)
      messages.flat_map do |message|
        @__reaction_dispatchers = []
        @__message_for_reaction = message
        explicit = Array(yield(message)).compact.reject { |value| value.is_a?(Dispatcher) }
        dispatched = @__reaction_dispatchers.map(&:message)
        explicit + dispatched
      end
    ensure
      @__reaction_dispatchers = []
      @__message_for_reaction = nil
    end

    class Dispatcher
      attr_reader :message

      def initialize(message)
        @message = message
      end

      def inspect = %(<#{self.class} #{@message}>)

      def at(datetime)
        @message = @message.at(datetime)
        self
      end

      def with_metadata(attrs = {})
        @message = @message.with_metadata(attrs)
        self
      end
    end

    # Queue a follow-up message from within a reaction block.
    #
    # The returned {Dispatcher} can be chained to delay the message
    # or add metadata before it is appended.
    #
    # @param message_class [Class, Symbol] messages class, or a symbol
    #   resolved via <tt>.[]</tt>
    # @param payload [Hash] message payload attributes
    # @return [Dispatcher] chainable wrapper around the dispatched message
    #
    # @example Dispatch by class
    #   reaction StudentEnrolled do |_state, event|
    #     dispatch(NotifyStudent, student_id: event.payload.student_id)
    #   end
    #
    # @example Dispatch by symbol with delay and metadata
    #   reaction StudentEnrolled do |_state, event|
    #     dispatch(:notify_student, student_id: event.payload.student_id)
    #       .with_metadata(channel: 'email')
    #       .at(Time.now + 300)
    #   end
    def dispatch(message_class, payload = {})
      message_class = self.class[message_class] if message_class.is_a?(Symbol)
      message = @__message_for_reaction
        .correlate(message_class.new(payload: payload))
        .with_metadata(producer: self.class.group_id)

      dispatcher = Dispatcher.new(message)
      @__reaction_dispatchers << dispatcher
      dispatcher
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_messages_for_react.each do |klass|
          subclass.handled_messages_for_react << klass
        end
        catch_all_react_events.each do |klass|
          subclass.catch_all_react_events << klass
        end
      end

      def handled_messages_for_react
        @handled_messages_for_react ||= []
      end

      def catch_all_react_events
        @catch_all_react_events ||= Set.new
      end

      # Register a reaction handler for one or more messages types.
      #
      # Accepts message classes, symbols resolved via <tt>.[]</tt>,
      # multiple arguments, or no arguments for a catch-all reaction across
      # all evolve types without an explicit handler.
      #
      # @example React to a specific event class
      #   reaction StudentEnrolled do |state, event|
      #     dispatch(NotifyStudent, student_id: event.payload.student_id)
      #   end
      #
      # @example React to a symbol-resolved message class
      #   reaction :student_enrolled do |state, event|
      #     dispatch(:notify_student, student_id: event.payload.student_id)
      #   end
      def reaction(*args, &block)
        case args
        in []
          handled_messages_for_evolve.each do |message_class|
            method_name = Sourced.message_method_name(PREFIX, message_class.to_s)
            next if instance_methods.include?(method_name.to_sym)

            catch_all_react_events << message_class
            reaction(message_class, &block)
          end

        in [Symbol => message_symbol]
          message_class = self[message_symbol].tap do |klass|
            raise(
              ArgumentError,
              "Cannot resolve message symbol #{message_symbol.inspect} for #{self}.reaction"
            ) unless klass
          end

          reaction(message_class, &block)
        in [Class => message_class] if message_class < Sourced::Message
          __validate_message_for_reaction!(message_class)
          handled_messages_for_react << message_class
          define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block) if block_given?
        in Array => values if values.none?(&:nil?)
          values.each { |value| reaction(value, &block) }
        else
          raise(
            ArgumentError,
            "Invalid arguments #{args.inspect} for #{self}.reaction"
          )
        end
      end

      def __validate_message_for_reaction!(_message_class)
        # no-op
      end
    end
  end
end
