module Sourced
  class AggregateRepo
    def initialize(event_store:)
      @event_store = event_store
      reset!
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
        aggr = aggregate_class.new
        aggr.load_from(stream)
        aggregates[aggr.id] = aggr
        aggr
      end
    end

    private
    attr_reader :event_store, :aggregates

    def reset!
      @aggregates = {}
    end

    def clear_events
      aggregates.values.flat_map(&:clear_events)
    end
  end
end
