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
          send(method_name, event) if respond_to?(method_name)
        end
      in [obj, events]
        state = obj
        events.each do |event|
          method_name = Sors.message_method_name(Evolve::PREFIX, event.class.to_s)
          send(method_name, state, event) if respond_to?(method_name)
        end
      end

      state
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

      def evolve(event_type, &block)
        handled_events_for_evolve << event_type unless event_type.is_a?(Symbol)
        block = NOOP_HANDLER unless block_given?
        define_method(Sors.message_method_name(Evolve::PREFIX, event_type.to_s), &block)
      end
    end
  end
end
