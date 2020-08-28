# frozen_string_literal: true

require 'thread'

module Sourced
  class PollingEventStore < SimpleDelegator
    def initialize(event_store, wait = 5)
      super event_store
      @wait = wait
    end

    def stream(after: nil)
      Enumerator.new do |y|
        loop do
          page = __getobj__.filter(after: after)
          page.each do |evt|
            after = evt.id
            y << evt
          end
          sleep @wait
        end
      end
    end
  end

  class Reactor
    EventProcessed = Event.define('sourced.reactor.event_processed') do
      field(:event_id).type(:uuid).present
    end

    class Session < EntitySession
      entity do |id|
        { id: id, last_processed_event_id: nil, date: nil }
      end
      projector do
        on Sourced::Reactor::EventProcessed do |state, evt|
          state[:date] = Time.now.utc
          state[:last_processed_event_id] = evt.payload.event_id
        end
      end
    end

    def self.run(*args)
      new(*args).run
    end

    def initialize(callable, name:, event_store:, save_every: 1, save_after_seconds: 10)
      @callable = callable
      @name = name
      @id = name # will include subscriber group info, eventually
      @event_store = PollingEventStore.new(event_store, 2) unless event_store.respond_to?(:stream)
      @save_every = save_every
      @save_after_seconds = save_after_seconds
      @repo = EntityRepo.new(Session, event_store: event_store)
      @mutex = Mutex.new
    end

    def run
      puts "Starting reactor #{id}"
      # Get latest state for this reactor
      session = repo.load(id)

      last_event = nil

      trap "SIGINT" do
        Thread.new do
          puts "Exiting"
          if last_event && session.entity[:last_processed_event_id] != last_event.id
            save_progress session, last_event.id, repo do
              puts "Auto saving on exit at #{session.seq}"
            end
          end
        end.join
        exit 130
      end

      Thread.new do
        loop do
          sleep save_after_seconds
          if last_event && session.entity[:last_processed_event_id] != last_event.id
            save_progress session, last_event.id, repo do
              puts "Auto saving at #{session.seq}"
            end
          end
        end
      end

      # Start stream from last position
      # We'll support different run strategies later
      # EventStore needs to support proper streaming
      # with category filter (ie. "orders") and subscriber groups
      # stream = event_store.filter(after: session.entity[:last_processed_event_id])
      stream = event_store.stream(after: session.entity[:last_processed_event_id])

      # Consume stream
      count = 0
      stream.each do |evt|
        next if evt.is_a?(EventProcessed) # this should not be necessary
        callable.call(evt)
        last_event = evt
        count += 1
        # Store latest position periodically
        if count == save_every
          save_progress session, evt.id, repo do
            count = 0
            puts "Saving reactor at #{session.seq}"
          end
        end
      end

      # Handle signals, interrupts, etc
    end

    private

    attr_reader :callable, :name, :id, :event_store, :repo, :save_every, :save_after_seconds, :mutex

    def save_progress(session, event_id, repo, &_block)
      mutex.synchronize {
        session.apply EventProcessed, payload: { event_id: event_id }
        repo.persist(session)
        yield if block_given?
      }
    end
  end
end
