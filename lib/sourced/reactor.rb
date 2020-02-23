# frozen_string_literal: true

module Sourced
  class Reactor
    EventProcessed = Event.define('sourced.reactor.event_processed') do
      field(:event_id).type(:uuid).present
    end

    class Session < EntitySession
      entity do |id|
        { id: id, last_processed_event_id: nil, date: nil }
      end
      projector do
        on Sourced::Reactor::EventProcessed do |evt, state|
          state[:date] = Time.now.utc
          state[:last_processed_event_id] = evt.payload.event_id
        end
      end
    end

    def self.run(*args)
      new(*args).run
    end

    def initialize(callable, name:, event_store:, save_interval: 1)
      @callable = callable
      @name = name
      @id = name # will include subscriber group info, eventually
      @event_store = event_store
      @save_interval = save_interval
      @repo = EntityRepo.new(event_store: event_store)
    end

    def run
      puts "Starting reactor #{id}"
      # Get latest state for this reactor
      session = repo.load(id, Session)

      # Start stream from last position
      # We'll support different run strategies later
      # EventStore needs to support proper streaming
      # with category filter (ie. "orders") and subscriber groups
      puts "querying events after #{session.entity[:last_processed_event_id]}"
      stream = event_store.filter(after: session.entity[:last_processed_event_id])

      # Consume stream
      count = 0
      stream.each do |evt|
        next if evt.is_a?(EventProcessed) # this should not be necessary
        callable.call(evt)
        count += 1
        # Store latest position periodically
        if count == save_interval
          count = 0
          session.apply EventProcessed, payload: { event_id: evt.id }
          puts "Saving reactor at #{session.seq}"
          repo.persist(session)
        end
      end

      # Handle signals, interrupts, etc
    end

    private

    attr_reader :callable, :name, :id, :event_store, :repo, :save_interval
  end
end
