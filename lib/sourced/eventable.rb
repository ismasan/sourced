module Sourced
  module Eventable
    def self.included(base)
      base.extend ClassMethods
    end

    def apply(event, collect: true)
      self.class.handlers[event.topic].each do |record|
        before_apply(event)
        deps = resolve_handler_dependencies(event, record.options)
        instance_exec(event, *deps, &record.handler)
        events << event if collect
      end
    end

    def events
      @events ||= []
    end

    def clear_events
      evts = @events.clone
      @events = []
      evts
    end

    private
    def before_apply(_)

    end

    def resolve_handler_dependencies(*_)

    end

    Record = Struct.new(:handler, :options)

    module ClassMethods
      def inherited(subclass)
        handlers.each do |key, list|
          subclass.handlers[key] = list
        end
      end

      def on(event_type, opts = {},  &block)
        key = event_type.respond_to?(:topic) ? event_type.topic : event_type.to_s
        handlers[key] << Record.new(block, opts)
      end

      def handlers
        @handlers ||= Hash.new{|h, k| h[k] = [] }
      end

      def topics
        handlers.keys
      end
    end
  end
end
