# frozen_string_literal: true

module Sourced
  class EntityRepo
    def initialize(event_store: MemEventStore.new, subscribers: [])
      @event_store = event_store
      @subscribers = subscribers
    end

    def build(id, entity_session_builder)
      entity_session_builder.load(id, [])
    end

    def load(id, entity_session_builder, **opts)
      stream = event_store.by_entity_id(id, opts)
      entity_session_builder.load(id, stream)
    end

    def persist(entity_session)
      entity_session.commit do |last_committed_seq, events|
        event_store.transaction do
          events = persist_events(events, expected_seq: last_committed_seq)
          dispatch(events, entity_session.entity)
        end
      end
    end

    def persist_events(events, expected_seq: nil)
      event_store.append(events, expected_seq: expected_seq)
    end

    private

    attr_reader :event_store, :subscribers

    def dispatch(events, entity)
      # Subscribers should not be dependent on eachother,
      # so this could potentially write concurrently to all of them.
      subscribers.each do |sub|
        sub.call(events, entity)
      end

      events
    end
  end
end
