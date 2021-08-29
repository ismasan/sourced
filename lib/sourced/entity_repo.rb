# frozen_string_literal: true

require 'sourced/committer_with_originator'

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

    def persist(stage, &block)
      stage.commit do |last_committed_seq, events, entity|
        event_store.transaction do
          events = persist_events(events, expected_seq: last_committed_seq)
          block.call(entity, events) if block_given?
          dispatch(entity, events)
        end
      end
    end

    def persist_with_originator(stage, originator_event, &block)
      persist(CommitterWithOriginator.new(originator_event, stage), &block)
    end

    def persist_events(events, expected_seq: nil)
      event_store.append(events, expected_seq: expected_seq)
    end

    private

    attr_reader :stage_builder, :event_store, :subscribers

    def dispatch(entity, events)
      # Subscribers should not be dependent on eachother,
      # so this could potentially write concurrently to all of them.
      subscribers.each do |sub|
        sub.call(events, entity)
      end

      events
    end
  end
end
