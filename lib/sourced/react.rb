# frozen_string_literal: true

module Sourced
  # This mixin provides a .react macro to register
  # event handlers for a class
  # These event handlers are "reactions", ie. they react to
  # events by producing new commands which will initiate new Decider flows.
  # More here: https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/#3-react
  #
  # From the outside, this mixin exposes the Reactor interface
  #
  #  .handle_events(Array<Sourced::Event>) Array<Sourced::Command>
  #
  # Example:
  #
  #  class Saga
  #    include Sourced::React
  #
  #    # React to an event and return a new command.
  #    # This command will be scheduled for processing by a Decider.
  #    # Using Sourced::Event#follow copies over metadata from the event
  #    # including causation and correlation IDs.
  #    reaction SomethingHappened do |event|
  #      event.follow(DoSomethingElse, field1: 'value1')
  #    end
  #  end
  module React
    PREFIX = 'reaction'
    REACTION_WITH_STATE_PREFIX = 'reaction_with_state'

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
        send(method_name, event) if respond_to?(method_name)
      end
    end

    def react_with_state(events, state)
      __handling_reactions(events) do |event|
        method_name = Sourced.message_method_name(React::REACTION_WITH_STATE_PREFIX, event.class.to_s)
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
        handled_events_for_react.each do |evt_type|
          subclass.handled_events_for_react << evt_type
        end
      end

      # Override this with extend Sourced::Consumer
      def consumer_info
        Sourced::Consumer::ConsumerInfo.new(group_id: name)
      end

      # These two are the Reactor interface
      # expected by Worker
      def handle_events(_events, replaying: false)
        raise NotImplementedError, "implement .handle_events(Array<Event>, replaying: Boolean) in #{self}"
      end

      def handled_events_for_react
        @handled_events_for_react ||= []
      end

      # Define a reaction to an event
      # @example
      #   reaction SomethingHappened do |event|
      #     stream = stream_for(event)
      #     # stream = stream_for("new-stream-id")
      #     stream.command DoSomethingElse
      #   end
      #
      # If the block defines two arguments, it will be registered as a reaction with state.
      # These handlers will load the decider's state from past events, and yield the state and the event to the block.
      # See .reaction_with_state
      # @example
      #   reaction SomethingHappened do |state, event|
      #     if state[:count] % 3 == 0
      #       steam_for(event).command DoSomething
      #     end
      #   end
      #
      # @param event_class [Class<Sourced::Message>]
      # @yield [Sourced::Event]
      # @return [void]
      def reaction(event_class = nil, &block)
        if block_given? && block.arity == 2 # state, event
          return reaction_with_state(event_class, &block)
        end

        unless event_class.is_a?(Class) && event_class < Sourced::Message
          raise ArgumentError,
                "Invalid argument #{event_class.inspect} for #{self}.reaction"
        end

        handled_events_for_react << event_class
        define_method(Sourced.message_method_name(React::PREFIX, event_class.to_s), &block) if block_given?
      end

      # @param event_name [Class]
      # @yield [Object, Sourced::Event]
      # @return [void]
      def reaction_with_state(event_class = nil, &block)
        if event_class.nil?
          # register a reaction for all handled events
          # except ones that have custom handlers
          handled_events_for_evolve.each do |evt_class|
            method_name = Sourced.message_method_name(React::REACTION_WITH_STATE_PREFIX, evt_class.to_s)
            if !instance_methods.include?(method_name.to_sym)
              reaction_with_state(evt_class, &block)
            end
          end

          return
        end

        unless event_class.is_a?(Class) && event_class < Sourced::Message
          raise ArgumentError,
                "Invalid argument #{event_class.inspect} for #{self}.reaction_with_state"
        end

        raise ArgumentError, '.reaction_with_state expects a block with |state, event|' unless block.arity == 2

        handled_events_for_react << event_class
        define_method(Sourced.message_method_name(React::REACTION_WITH_STATE_PREFIX, event_class.to_s), &block) if block_given?
      end
    end
  end
end
