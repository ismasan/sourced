# frozen_string_literal: true

module Sourced
  class Configuration
    attr_writer :event_store

    def event_store
      @event_store ||= Sourced::MemEventStore.new
    end

    def aggregate_repo
      @aggregate_repo ||= Sourced::AggregateRepo.new(event_store: event_store)
    end
  end
end
