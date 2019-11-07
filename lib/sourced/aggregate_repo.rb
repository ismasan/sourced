module Sourced
  class AggregateRepo
    def initialize(event_store: MemEventStore.new)
      @event_store = event_store
    end

    def load(id, aggregate_class, opts = {})
      stream = event_store.by_aggregate_id(id, opts)
      aggr = aggregate_class.new(id)
      aggr.load_from(stream)
      aggr
    end

    def build(aggregate_class)
      aggregate_class.new(id: Sourced.uuid)
    end

    def persist(aggregate)
      event_store.append(aggregate.clear_events)
    end

    private

    attr_reader :event_store
  end
end
