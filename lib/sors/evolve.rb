# frozen_string_literal: true

module Sors
  module Evolve
    PREFIX = 'evolution'

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def handled_events = self.class.handled_events

    def evolve(state, events)
      events.each do |event|
        __handle_evolution_any(state, event)
        method_name = Sors.message_method_name(PREFIX, event.class.to_s)
        send(method_name, state, event) if respond_to?(method_name)
      end

      state
    end

    private def handle_evolution_any(state, _event)
      state
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_events.each do |evt_type|
          subclass.handled_events << evt_type
        end
      end

      def handle_evolve(state, events)
        new.evolve(state, events)
      end

      def handled_events
        @handled_events ||= []
      end

      def evolve(event_type, &block)
        handled_events << event_type unless event_type.is_a?(Symbol)
        define_method(Sors.message_method_name(PREFIX, event_type.to_s), &block)
      end
    end
  end
end
