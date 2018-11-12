module Sourced
  class AggregateRepo
    def initialize(event_store: MemEventStore.new)
      @event_store = event_store
      reset!
    end

    def add(id, aggregate_class)
      if aggr = aggregates[id]
        aggr
      else
        register_aggregate(id, aggregate_class)
      end
    end

    def load(id, aggregate_class, opts = {})
      catchup = !!opts.delete(:catchup)
      # if aggregate already cached, should we use that one and only load any new events?
      if aggr = aggregates[id]
        # catch up with new events, if any
        if catchup
          stream = event_store.by_aggregate_id(id, from: aggr.version)
          aggr.load_from stream
        end
        aggr
      else
        stream = event_store.by_aggregate_id(id, opts)
        aggr = register_aggregate(id, aggregate_class)
        aggr.load_from(stream)
        aggr
      end
    end

    def clear_events
      evts = events.dup
      events.clear
      evts
    end

    private
    attr_reader :event_store, :aggregates, :events

    def register_aggregate(id, aggregate_class)
      aggr = aggregate_class.new(id, events: events)
      raise InvalidAggregateError, 'aggregates must set :id on initialize' unless aggr.id == id
      aggregates[id] = aggr
      aggr
    end

    def reset!
      @aggregates = {}
      @events = []
    end
  end
end
