module Sourced
  module Aggregate
    def self.included(base)
      base.send :include, Eventable
    end
    include Eventable

    attr_reader :id

    def version
      @version ||= 0
    end

    def load_from(stream)
      stream.each do |evt|
        apply evt, collect: false
      end
      self
    end

    private
    def before_apply(event)
      @version = event.version
    end

    def emit(event_or_class, attrs = {})
      event = if event_or_class.respond_to?(:instance)
        event_or_class.instance(next_event_attrs.merge(attrs))
      else
        event_or_class
      end

      apply event
    end

    def next_event_attrs
      {
        aggregate_id: id,
        version: version + 1,
      }
    end
  end
end
