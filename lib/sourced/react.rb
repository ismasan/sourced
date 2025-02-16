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

    # @param events [Array<Sourced::Event>]
    # @return [Array<Sourced::Command>]
    def react(events)
      @__commands_after_reaction = []
      events.each do |event|
        __handle_reaction(event)
      end
      cmds = @__commands_after_reaction.dup
      @__commands_after_reaction.clear
      cmds
    end

    private

    def __handle_reaction(event)
      method_name = Sourced.message_method_name(React::PREFIX, event.class.to_s)
      return [] unless respond_to?(method_name)

      @__event_for_reaction = event
      send(method_name, event)
    end

    # TODO: Actor can do #command(:some_command)
    def command(command_class, payload = {}, &)
      cmd = @__event_for_reaction
            .follow(command_class, payload)
            .with_metadata(producer: self.class.consumer_info.group_id)

      cmd = yield(cmd) if block_given?
      @__commands_after_reaction << cmd
      cmd
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

      # A standalone reactor doesn't have its own Event
      # structs defined via .event(:event_name, payload_schema)
      # So it can only take qualified event classes
      # Decider can override this method to provide symbol-based event names
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
