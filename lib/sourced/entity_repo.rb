# frozen_string_literal: true

module Sourced
  class EntityRepo
    def initialize(event_store: MemEventStore.new)
      @event_store = event_store
    end

    def build(id, entity_session_builder)
      entity_session_builder.load(id, [])
    end

    def load(id, entity_session_builder, **opts)
      stream = event_store.by_entity_id(id, opts)
      entity_session_builder.load(id, stream)
    end

    def persist(entity_session)
      persist_events(entity_session.clear_events)
    end

    def persist_events(events)
      event_store.append(events)
      events
    end

    private

    attr_reader :event_store
  end
end
