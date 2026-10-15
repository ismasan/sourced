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
      handlers = self.class.reaction_handlers
      __handling_reactions(Array(messages)) do |message|
        method_name = handlers[message.class]
        method_name ? Array(send(method_name, context, message)).compact : EMPTY_ARRAY
      end
    end

    # Whether this reactor registers a reaction handler for +message+.
    # Static type routing: it only asks whether a handler exists.
    #
    # @param message [Sourced::Message]
    # @return [Boolean]
    def reacts_to?(message)
      self.class.handled_messages_for_react.include?(message.class)
    end

    # Whether a registered reaction should actually run for +message+.
    # Dynamic policy, evaluated after {#reacts_to?} for every claimed message.
    #
    # By default reactions are skipped while replaying, so that re-processing
    # a partition (after a consumer group reset, or an offset rewind) does not
    # re-dispatch commands that already ran.
    #
    # Override to react on replay, or to gate on context or payload:
    #
    #   # always react, even on replay
    #   def should_react?(_context, _message, replaying: false) = true
    #
    #   # react on replay only for messages the projection hasn't seen
    #   def should_react?(context, message, replaying: false)
    #     return true unless replaying
    #     !context[:notified].include?(message.payload.student_id)
    #   end
    #
    # +context+ is the reactor's context with +message+ already applied. In
    # projectors the whole claimed batch has been applied, not just +message+.
    #
    # @param context [Object] reactor context
    # @param message [Sourced::Message] the message about to be reacted to
    # @param replaying [Boolean] whether this batch has been processed before
    # @return [Boolean]
    def should_react?(_context, _message, replaying: false)
      !replaying
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
    # @param message_class [Class] message class
    # @param payload [Hash] message payload attributes
    # @return [Dispatcher] chainable wrapper around the dispatched message
    #
    # @example Dispatch
    #   reaction StudentEnrolled do |_context, event|
    #     dispatch(NotifyStudent, student_id: event.payload.student_id)
    #   end
    #
    # @example Dispatch with delay and metadata
    #   reaction StudentEnrolled do |_context, event|
    #     dispatch(NotifyStudent, student_id: event.payload.student_id)
    #       .with_metadata(channel: 'email')
    #       .at(Time.now + 300)
    #   end
    def dispatch(message_class, payload = {})
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
        subclass.reaction_handlers.merge!(reaction_handlers)
      end

      # @return [Hash{Class => Symbol}] each reacted-to message class's reaction method
      def reaction_handlers
        @reaction_handlers ||= {}
      end

      def handled_messages_for_react
        @handled_messages_for_react ||= []
      end

      def catch_all_react_events
        @catch_all_react_events ||= Set.new
      end

      # Register a reaction handler for one or more messages types.
      #
      # Accepts one or more message classes, or no arguments for a
      # catch-all reaction across all evolve types without an explicit handler.
      #
      # @example React to a specific event class
      #   reaction StudentEnrolled do |context, event|
      #     dispatch(NotifyStudent, student_id: event.payload.student_id)
      #   end
      #
      # @example React to several message classes
      #   reaction StudentEnrolled, StudentRejoined do |context, event|
      #     dispatch(NotifyStudent, student_id: event.payload.student_id)
      #   end
      def reaction(*args, &block)
        case args
        in []
          handled_messages_for_evolve.each do |message_class|
            next if reaction_handlers.key?(message_class)

            catch_all_react_events << message_class
            reaction(message_class, &block)
          end

        in [Class => message_class] if message_class < Sourced::Message
          __validate_message_for_reaction!(message_class)
          handled_messages_for_react << message_class
          if block_given?
            reaction_handlers[message_class] = define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block)
          end
        in [_, _, *] => values
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
