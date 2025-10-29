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

    def self.included(base)
      super
      base.extend ClassMethods
    end

    # Helper class to collect commands to be dispatched
    # after runing a reaction block.
    # See #reaction
    class StreamDispatcher
      attr_reader :stream_id, :commands

      def initialize(stream_id, source_event, producer_reactor, target_reactor)
        @stream_id = stream_id
        @source_event = source_event
        @producer_reactor = producer_reactor
        @target_reactor = target_reactor
        @commands = []
      end

      def inspect = "#<#{self.class} stream_id: #{stream_id}, source_event: #{source_event.inspect}>"

      # Build a command instance
      # with metadata from the source event
      #
      # @param command_class [Class, Symbol]
      # @param payload [#to_h]
      # @yieldparam cmd [Sourced::Command]
      # @return [Sourced::Command]
      def command(command_class, payload = {}, &)
        command_class = target_reactor[command_class] if command_class.is_a?(Symbol)

        cmd = source_event
              .follow_with_stream_id(command_class, stream_id, payload)
              .with_metadata(producer: producer_reactor.consumer_info.group_id)

        cmd = yield(cmd) if block_given?
        @commands << cmd
        cmd
      end

      private

      attr_reader :source_event, :producer_reactor, :target_reactor
    end

    # @param events [Array<Sourced::Event>]
    # @return [Array<Sourced::Command>]
    def react(events)
      __handling_reactions(events) do |event|
        method_name = Sourced.message_method_name(React::PREFIX, event.class.to_s)
        send(method_name, state, event) if respond_to?(method_name)
      end
    end

    private

    def __handling_reactions(events, &)
      @__stream_dispatchers = {}
      events.each do |event|
        @__event_for_reaction = event
        yield event
      end
      cmds = @__stream_dispatchers.values.flat_map(&:commands)
      @__stream_dispatchers.clear
      cmds
    end

    # Helper to build a StreamDispatcher from within a reaction block
    # Works with Strings, or anything that responds to #stream_id
    # @example
    #   stream = stream_for(event)
    #   stream = stream_for("new-stream-id")
    #   stream.command DoSomething, name: "value"
    #   # with block to refine command
    #   stream.command DoSomething do |cmd|
    #     cmd.with_metadata(greeting: 'Hi!')
    #     cmd.delay(Time.zone.now + 1.day)
    #   end
    #
    # @param stream_id [String, #stream_id]
    # @return [StreamDispatcher]
    def stream_for(stream_id)
      target_reactor = stream_id.respond_to?(:resolve_message_class) ? stream_id : self.class
      stream_id = stream_id.stream_id if stream_id.respond_to?(:stream_id)
      key = [stream_id, @__event_for_reaction.id]
      @__stream_dispatchers[key] ||= StreamDispatcher.new(stream_id, @__event_for_reaction, self.class, target_reactor)
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
      # @param event_class [Class<Sourced::Message>]
      # @yield [Object, Sourced::Event]
      # @return [void]
      def reaction(event_class = nil, &block)
        __validate_message_for_reaction!(event_class)
        unless event_class.is_a?(Class) && event_class < Sourced::Message
          raise ArgumentError,
                "Invalid argument #{event_class.inspect} for #{self}.reaction"
        end

        handled_messages_for_react << event_class
        define_method(Sourced.message_method_name(React::PREFIX, event_class.to_s), &block) if block_given?
      end

      # Run this hook before registering a reaction
      # Actor can override this to make sure that the same message is not
      # also handled as a command
      def __validate_message_for_reaction!(event_class)
        # no-op.
      end
    end
  end
end
