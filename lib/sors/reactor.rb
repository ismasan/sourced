# frozen_string_literal: true

module Sors
  class Reactor
    class << self
      def handled_events
        @handled_events ||= []
      end

      def react(event_type, &block)
        handled_events << event_type
        define_method(message_method_name(event_type.name), &block) if block_given?
      end

      def call(...)
        new.call(...)
      end

      def handle(events)
        new.handle(events)
      end

      def message_method_name(name)
        "__handle_#{name.split('::').map(&:downcase).join('_')}"
      end
    end

    def call(events)
      events.flat_map { |event| handle(event) }
    end

    def handle(event)
      method_name = self.class.message_method_name(event.class.name)
      return [] unless respond_to?(method_name)

      cmds = send(method_name, event)
      [cmds].flatten.compact
    end
  end
end
