module Sourced
  module Handler
    def self.included(base)
      base.send(:include, Eventable)
    end

    def topics
      self.class.handlers.keys
    end

    def call(cmd, *args)
      clear_events
      apply(cmd, collect: false)
      clear_events
    end

    private
    def emit(event_or_class, attrs = {})
      event = if event_or_class.respond_to?(:instance)
        event_or_class.instance(attrs)
      else
        event_or_class
      end

      events << event
    end
  end
end
