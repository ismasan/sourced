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

    def self.included(base)
      super
      base.extend ClassMethods
    end

    # Helper class to collect commands to be dispatched
    # after runing a reaction block.
    # See #reaction
    class StreamDispatcher
      attr_reader :stream_id, :commands

      def initialize(stream_id, source_event, reactor_class)
        @stream_id = stream_id
        @source_event = source_event
        @reactor_class = reactor_class
        @commands = []
      end

      # Build a command instance
      # with metadata from the source event
      #
      # @param command_class [Class, Symbol]
      # @param payload [#to_h]
      # @yieldparam cmd [Sourced::Command]
      # @return [Sourced::Command]
      def command(command_class, payload = {}, &)
        command_class = reactor_class[command_class] if command_class.is_a?(Symbol)

        cmd = source_event
              .follow(command_class, payload)
              .with_metadata(producer: reactor_class.consumer_info.group_id)

        cmd = yield(cmd) if block_given?
        @commands << cmd
        cmd
      end

      private

      attr_reader :source_event, :reactor_class
    end

    # @param events [Array<Sourced::Event>]
    # @return [Array<Sourced::Command>]
    def react(events)
      @__stream_dispatchers = {}
      events.each do |event|
        __handle_reaction(event)
      end
      cmds = @__stream_dispatchers.values.flat_map(&:commands)
      @__stream_dispatchers.clear
      cmds
    end

    private

    def __handle_reaction(event)
      method_name = Sourced.message_method_name(React::PREFIX, event.class.to_s)
      return [] unless respond_to?(method_name)

      @__event_for_reaction = event
      send(method_name, event)
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
      stream_id = stream_id.stream_id if stream_id.respond_to?(:stream_id)
      key = [stream_id, @__event_for_reaction.id]
      @__stream_dispatchers[key] ||= StreamDispatcher.new(stream_id, @__event_for_reaction, self.class)
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
      def handle_events(_events)
        raise NoMethodError, "implement .handle_events(Array<Event>) in #{self}"
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
      # @param event_class [Class<Sourced::Message>]
      def reaction(event_class, &block)
        unless event_class.is_a?(Class) && event_class < Sourced::Message
          raise ArgumentError,
                "Invalid argument #{event_class.inspect} for #{self}.react"
        end

        handled_events_for_react << event_class
        define_method(Sourced.message_method_name(React::PREFIX, event_class.to_s), &block) if block_given?
      end
    end
  end
end
