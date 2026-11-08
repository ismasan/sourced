# frozen_string_literal: true

require 'set'

module Sourced
  # React mixin for reactors.
  # Supports the same dispatch-based reaction DSL as Sourced::React,
  # adapted to Sourced's stream-less messages.
  module React
    PREFIX = 'sourced_reaction'
    EMPTY_ARRAY = [].freeze

    # Raised when a reaction appends a message its declaration doesn't
    # include, or appends nothing when its outputs don't include +nil+.
    UndeclaredMessageError = Class.new(UndeclaredOutputError)

    def self.included(base)
      super
      base.extend ClassMethods
    end

    # Run reaction handlers for one or more messages.
    # Supports both explicit message returns and dispatch(...) calls.
    def react(messages)
      __handling_reactions(Array(messages)) { |message| __run_reaction(message) }
    end

    # Whether this reactor registers a reaction handler for +message+.
    # Static type routing: it only asks whether a handler exists.
    #
    # @param message [Sourced::Message]
    # @return [Boolean]
    def reacts_to?(message)
      self.class.handled_messages_for_react.include?(message.to_message.class)
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

    # Run +message+'s reaction handler, if it has one, holding what it
    # appends (returned or dispatched) to its declared outputs.
    def __run_reaction(message)
      klass = message.to_message.class
      method_name = self.class.reaction_handlers[klass]
      return EMPTY_ARRAY unless method_name

      started = @__reaction_dispatchers.size
      returned = Array(send(method_name, context, message)).compact
      dispatched = @__reaction_dispatchers.drop(started).map(&:message)
      __check_reaction_outputs!(klass, returned.reject { |value| value.is_a?(Dispatcher) } + dispatched)
      returned
    end

    def __check_reaction_outputs!(klass, appended)
      outputs = self.class.reaction_outputs.fetch(klass)
      appended.each do |msg|
        next if outputs.include?(msg.to_message.class)

        raise UndeclaredMessageError, "#{self.class}: the #{klass} reaction appended #{msg.to_message.class}, " \
                                      "which it doesn't declare (#{outputs})"
      end
      return if appended.any? || outputs.optional

      raise UndeclaredMessageError, "#{self.class}: the #{klass} reaction appended nothing; " \
                                    "declare #{outputs} | nil if it may append none"
    end

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
    #   reaction StudentEnrolled => NotifyStudent do |_context, event|
    #     dispatch(NotifyStudent, student_id: event.payload.student_id)
    #   end
    #
    # @example Dispatch with delay and metadata
    #   reaction StudentEnrolled => NotifyStudent do |_context, event|
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
        subclass.reaction_outputs.merge!(reaction_outputs)
      end

      # @return [Hash{Class => Symbol}] each reacted-to message class's reaction method
      def reaction_handlers
        @reaction_handlers ||= {}
      end

      # @return [Hash{Class => Outputs}] each reacted-to message class's declared outputs
      def reaction_outputs
        @reaction_outputs ||= {}
      end

      # @return [Array<Class>] every message class a reaction may append
      def dispatched_messages = reaction_outputs.values.flat_map(&:messages).uniq

      def handled_messages_for_react
        @handled_messages_for_react ||= []
      end

      def catch_all_react_events
        @catch_all_react_events ||= Set.new
      end

      # Register a reaction handler, and what it may append: a message class,
      # a union of them, with +| nil+ when it may append none.
      #
      # The reacted-to side is a message class (or anything whose
      # +to_message_class+ returns one, see {Sourced::Message.to_message_class}),
      # a union of them (+A | B+), or +:any+ for a catch-all reaction across
      # all evolve types without a handler of their own. Handlers are looked up
      # by +message.to_message.class+. Without a block, +reaction A+ only
      # subscribes to +A+.
      #
      # @example React to an event
      #   reaction StudentEnrolled => NotifyStudent do |context, event|
      #     dispatch(NotifyStudent, student_id: event.payload.student_id)
      #   end
      #
      # @example React to several message classes, maybe appending nothing
      #   reaction StudentEnrolled | StudentRejoined => NotifyStudent | nil do |context, event|
      #     dispatch(NotifyStudent, student_id: event.payload.student_id) if context[:notify]
      #   end
      #
      # @example React to every evolved type without a handler
      #   reaction any: Audit do |context, event|
      #     dispatch(Audit, type: event.type)
      #   end
      #
      # @raise [ArgumentError] for a block without declared outputs, or a
      #   declaration without a block
      def reaction(*args, &block)
        case args
        in [Hash => declaration] if declaration.size == 1
          raise ArgumentError, "#{self}.reaction #{declaration.inspect} needs a block" unless block

          reacted, declared = declaration.first
          outputs = begin
            Outputs.parse(declared)
          rescue ArgumentError => e
            raise ArgumentError, "#{self}.reaction #{reacted}: #{e.message}"
          end
          if reacted == :any
            __react_to_any(outputs, &block)
          else
            __reacted_classes(reacted).each { |message_class| __react_to(message_class, outputs, &block) }
          end
        in _ if block
          raise ArgumentError, "#{self}.reaction declares what it appends: one pair, ex. " \
                               'reaction Event => Command, Event | Other => Command | nil, or any: Command; ' \
                               "got #{args.map(&:inspect).join(', ')}"
        in [_, *] => values
          values.each { |value| __subscribe(value.to_message_class) }
        end
      end

      # @api private
      def __react_to_any(outputs, &block)
        # What it evolves from, however it's partitioned (see Evolve.evolve_selector).
        evolve_selector.types.each do |message_class|
          next if reaction_handlers.key?(message_class)

          catch_all_react_events << message_class
          __react_to(message_class, outputs, &block)
        end
      end

      # @api private
      def __react_to(message_class, outputs, &block)
        __subscribe(message_class)
        reaction_outputs[message_class] = outputs
        method_name = Sourced.message_method_name(PREFIX, message_class.to_s)
        reaction_handlers[message_class] = define_method(method_name, &block)
      end

      # @api private
      def __subscribe(message_class)
        __validate_message_for_reaction!(message_class)
        handled_messages_for_react << message_class unless handled_messages_for_react.include?(message_class)
      end

      # The classes a reaction's left side names: a union of message classes
      # (+Plumb::Or+), or of machine states and parts (+Machine::Targets+).
      # @api private
      def __reacted_classes(reacted)
        case reacted
        in Plumb::Or then reacted.children.flat_map { __reacted_classes(it) }
        in Machine::Targets then reacted.members.flat_map { __reacted_classes(it) }
        in nil then raise ArgumentError, "#{self}.reaction: nil isn't a message class"
        else [reacted.to_message_class]
        end
      end

      def __validate_message_for_reaction!(_message_class)
        # no-op
      end
    end
  end
end
