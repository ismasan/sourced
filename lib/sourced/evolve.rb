# frozen_string_literal: true

module Sourced
  # This mixin provides an .event macro
  # to register event handlers for a class
  # These event handlers are "evolvers", ie. they evolve
  # a piece of state based on events.
  # More here: https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/#2-evolve
  #
  # Example:
  #
  #  class Projector
  #    include Sourced::Evolve
  #
  #    state do
  #      { status: 'new' }
  #    end
  #
  #    event SomethingHappened do |state, event|
  #      state[:status] = 'done'
  #    end
  #  end
  #
  #  pr = Projector.new
  #  state = pr.evolve([SomethingHappened.new])
  #  state[:status] # => 'done'
  #
  # From the outside, this mixin exposes .handled_messages_for_evolve
  #
  #  .handled_messages_for_evolve() Array<Sourced::Message>
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

    # Initialize in-memory state for this evolver.
    # Override this method to provide a custom initial state.
    # @return [Any] the initial state
    def init_state(_id)
      nil
    end

    def state
      @state ||= init_state(id)
    end

    # Override this in host class
    def id = nil

    # Apply a list of events to a piece of state
    # by running event handlers registered in this class
    # via the .event macro.
    #
    # @param events [Array<Sourced::Message>]
    # @return [Object]
    def evolve(events)
      Array(events).each do |event|
        method_name = Sourced.message_method_name(Evolve::PREFIX, event.class.to_s)
        # We might be evolving old events in history
        # even if we don't have handlers for them anymore
        # we still need to increment seq
        __update_on_evolve(event)
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

    private def __update_on_evolve(event)
      # Noop
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_messages_for_evolve.each do |evt_type|
          subclass.handled_messages_for_evolve << evt_type
        end
      end

      def handled_messages_for_evolve
        @handled_messages_for_evolve ||= []
      end

      # Define an initial state factory for this evolver.
      # @example
      #
      #   state do
      #     { status: 'new' }
      #   end
      #
      def state(&blk)
        define_method(:init_state, &blk)
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

        handled_messages_for_evolve << event_class
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
      #   # From another Evolver that responds to #handled_messages_for_evolve
      #   evolve_all CartAggregate do |state, event|
      #     state.updated_at = event.created_at
      #   end
      #
      # @param event_list [Array<Sourced::Message>, #handled_messages_for_evolve() [Array<Sourced::Message>}]
      def evolve_all(event_list, &block)
        event_list = event_list.handled_messages_for_evolve if event_list.respond_to?(:handled_messages_for_evolve)
        event_list.each do |event_type|
          event(event_type, &block)
        end
      end
    end
  end
end
