# frozen_string_literal: true

module Sourced
  class EntityRepo
    def initialize(stage_builder, event_store: MemEventStore.new, subscribers: [])
      @stage_builder = stage_builder
      @event_store = event_store
      @subscribers = subscribers
    end

    def build(id)
      stage_builder.load(id, [])
    end

    def load(id, **opts)
      stream = event_store.by_entity_id(id, opts)
      stage_builder.load(id, stream)
    end

    def persist(stage)
      stage.commit do |last_committed_seq, events, entity|
        event_store.transaction do
          events = persist_events(events, expected_seq: last_committed_seq)
          dispatch(events, entity)
        end
      end
    end

    def persist_events(events, expected_seq: nil)
      event_store.append(events, expected_seq: expected_seq)
    end

    private

    attr_reader :stage_builder, :event_store, :subscribers

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
