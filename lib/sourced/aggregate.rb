module Sourced
  class Aggregate
    include Eventable

    attr_reader :id, :events

    def initialize(id)
      @id = id
      @events = []
    end

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

    def next_event_attrs
      basic_event_attrs.merge({
        aggregate_id: id,
        version: version + 1,
      })
    end

    # override this in your classes
    # these props will be added to any events applied
    # by your Aggregate
    def basic_event_attrs
      {}
    end
  end
end
