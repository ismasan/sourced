# frozen_string_literal: true

module Sourced
  # This mixin provides an .event macro
  # to register event handlers for a class
  # These event handlers are "evolvers", ie. they evolve
  # a piece of state based on events.
  # More here: https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/#2-evolve
  #
  # From the outside, this mixin exposes the Reactor interface
  #
  #  .handle_events(Array<Sourced::Event>) Array<Sourced::Command>
  #
  # Example:
  #
  #  class Projector
  #    include Sourced::Evolve
  #
  #    event SomethingHappened do |state, event|
  #      state[:status] = 'done'
  #    end
  #  end
  #
  #  pr = Projector.new
  #  state = { status: 'new' }
  #  state = pr.evolve(state, [SomethingHappened.new])
  #  state[:status] # => 'done'
  #
  # It also provides a .before_evolve and .evolve_all macros
  # See comments in code for details.
  module Evolve
    PREFIX = 'evolution'
    NOOP_HANDLER = ->(*_) { nil }

    def self.included(base)
      super
      base.extend ClassMethods
    end

    # Apply a list of events to a piece of state
    # by running event handlers registered in this class
    # via the .event macro.
    #
    # @param state [Object]
    # @param events [Array<Sourced::Event>]
    # @return [Object]
    def evolve(state, events)
      events.each do |event|
        method_name = Sourced.message_method_name(Evolve::PREFIX, event.class.to_s)
        if respond_to?(method_name)
          before_evolve(state, event)
          send(method_name, state, event)
        end
      end

      state
    end

    private def before_evolve(*_)
      nil
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_events_for_evolve.each do |evt_type|
          subclass.handled_events_for_evolve << evt_type
        end
      end

      # The Reactor interface
      # expected by Worker
      def handle_events(_events, replaying: false)
        raise NotImplementedError, "implement .handle_events(Array<Event>, replaying: Boolean) in #{self}"
      end

      def handled_events_for_evolve
        @handled_events_for_evolve ||= []
      end

      # This module only accepts registering event handlers
      # with qualified event classes
      # Decider overrides this method to allow
      # defining event handlers with symbols
      # which are registered as Event classes in the decider namespace.
      # @example
      #
      #   event SomethingHappened do |state, event|
      #     state[:status] = 'done'
      #   end
      #
      # @param event_class [Sourced::Message]
      # @return [void]
      def event(event_class, &block)
        unless event_class.is_a?(Class) && event_class < Sourced::Message
          raise ArgumentError,
                "Invalid argument #{event_class.inspect} for #{self}.event"
        end

        handled_events_for_evolve << event_class
        block = NOOP_HANDLER unless block_given?
        define_method(Sourced.message_method_name(Evolve::PREFIX, event_class.to_s), &block)
      end

      # Run this block before any of the registered event handlers
      # Example:
      #   before_evolve do |state, event|
      #     state.udpated_at = event.created_at
      #   end
      def before_evolve(&block)
        define_method(:before_evolve, &block)
      end

      # Example:
      #   # With an Array of event types
      #   evolve_all [:event_type1, :event_type2] do |state, event|
      #     state.updated_at = event.created_at
      #   end
      #
      #   # From another Evolver that responds to #handled_events_for_evolve
      #   evolve_all CartAggregate do |state, event|
      #     state.updated_at = event.created_at
      #   end
      #
      # @param event_list [Array<Sourced::Message>, #handled_events_for_evolve() [Array<Sourced::Message>}]
      def evolve_all(event_list, &block)
        event_list = event_list.handled_events_for_evolve if event_list.respond_to?(:handled_events_for_evolve)
        event_list.each do |event_type|
          event(event_type, &block)
        end
      end
    end
  end
end
