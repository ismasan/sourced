# frozen_string_literal: true

require 'sourced/committer_with_originator'

module Sourced
  class EntityRepo
    def initialize(stage_builder, event_store: MemEventStore.new)
      @stage_builder = stage_builder
      @event_store = event_store
    end

    def build(id)
      stage_builder.load(id, [])
    end

    def load(id, **opts)
      stream = event_store.read_stream(id, **opts)
      stage_builder.load(id, stream)
    end

    def persist(stage, &block)
      stage.commit do |last_committed_seq, events, entity|
        event_store.transaction do
          events = persist_events(events, expected_seq: last_committed_seq)
          block.call(entity, events) if block_given?
          events
        end
      end
    end

    def persist_with_originator(stage, originator_event, &block)
      persist(CommitterWithOriginator.new(originator_event, stage), &block)
    end

    def persist_events(events, expected_seq: nil)
      return [] unless events.any?

      event_store.append_to_stream(events.first.stream_id, events, expected_seq: expected_seq)
    end

    private

    attr_reader :stage_builder, :event_store
  end
end
