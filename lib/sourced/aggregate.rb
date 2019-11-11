module Sourced
  class Aggregate
    include Eventable

    def self.build(id = Sourced.uuid)
      new(id)
    end

    attr_reader :id, :events, :last_event_id

    def initialize(id)
      @id = id
      @events = []
    end

    def seq
      @seq ||= 0
    end

    def load_from(stream)
      stream.each do |evt|
        apply evt, collect: false
      end
      self
    end

    def ==(other)
      other.id == id && other.last_event_id == last_event_id
    end

    def inspect
      %(<#{self.class.name}##{id} #{events.size} uncommitted events >)
    end

    private

    def before_apply(event)
      @seq = event.seq
      @last_event_id = event.id
    end

    def next_event_attrs
      basic_event_attrs.merge({
        aggregate_id: id,
        seq: seq + 1,
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
