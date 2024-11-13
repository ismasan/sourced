# frozen_string_literal: true

module Sourced
  module React
    PREFIX = 'reaction'

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def react(events)
      events.flat_map { |event| __handle_reaction(event) }
    end

    private

    def __handle_reaction(event)
      method_name = Sourced.message_method_name(React::PREFIX, event.class.to_s)
      return [] unless respond_to?(method_name)

      cmds = send(method_name, event)
      [cmds].flatten.compact
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_events_for_react.each do |evt_type|
          subclass.handled_events_for_react << evt_type
        end
      end

      # These two are the Reactor interface
      # expected by Worker
      def handle_events(_events)
        raise NoMethodError, "implement .handle_events(Array<Event>) in #{self}"
      end

      def handled_events_for_react
        @handled_events_for_react ||= []
      end

      def react(event_type, &block)
        handled_events_for_react << event_type unless event_type.is_a?(Symbol)
        define_method(Sourced.message_method_name(React::PREFIX, event_type.to_s), &block) if block_given?
      end
    end
  end
end
