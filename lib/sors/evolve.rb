# frozen_string_literal: true

module Sors
  module Evolve
    PREFIX = 'evolution'
    NOOP_HANDLER = ->(*_) { nil }

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def evolve(*args)
      state = self

      case args
      in [events]
        events.each do |event|
          method_name = Sors.message_method_name(Evolve::PREFIX, event.class.to_s)
          if respond_to?(method_name)
            before_evolve(event)
            send(method_name, event) 
        end
        end
      in [obj, events]
        state = obj
        events.each do |event|
          method_name = Sors.message_method_name(Evolve::PREFIX, event.class.to_s)
          if respond_to?(method_name)
            before_evolve(state, event)
            send(method_name, state, event)
          end
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

      # These two are the Reactor interface
      # expected by Worker
      def handle_events(_events)
        raise NoMethodError, "implement .handle_events(Array<Event>) in #{self}"
      end

      def handled_events_for_evolve
        @handled_events_for_evolve ||= []
      end

      # Example:
      #   evolve :event_type do |event|
      #     @updated_at = event.created_at
      #   end
      #
      # @param event_type [Sors::Message]
      def evolve(event_type, &block)
        handled_events_for_evolve << event_type unless event_type.is_a?(Symbol)
        block = NOOP_HANDLER unless block_given?
        define_method(Sors.message_method_name(Evolve::PREFIX, event_type.to_s), &block)
      end

      # Run this block before any of the registered event handlers
      # Example:
      #   before_evolve do |event|
      #     @updated_at = event.created_at
      #   end
      def before_evolve(&block)
        define_method(:before_evolve, &block)
      end

      # Example:
      #   # With an Array of event types
      #   evolve_all [:event_type1, :event_type2] do |event|
      #     @updated_at = event.created_at
      #   end
      #
      #   # From another Evolver that responds to #handled_events_for_evolve
      #   evolve_all CartAggregate do |event|
      #     @updated_at = event.created_at
      #   end
      #
      # @param event_list [Array<Sors::Message>, #handled_events_for_evolve() {Array<Sors::Message>}]
      def evolve_all(event_list, &block)
        event_list = event_list.handled_events_for_evolve if event_list.respond_to?(:handled_events_for_evolve)
        event_list.each do |event_type|
          evolve(event_type, &block)
        end
      end
    end
  end
end
