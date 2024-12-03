# frozen_string_literal: true

module Sourced
  class Projector
    include Evolve
    include Sync
    extend Consumer

    class << self
      def handled_events = handled_events_for_evolve
    end

    attr_reader :id, :seq, :state

    def initialize(id, backend: Sourced.config.backend, logger: Sourced.config.logger)
      @id = id
      @seq = 0
      @backend = backend
      @logger = logger
      @state = init_state(id)
    end

    def inspect
      %(<#{self.class} id:#{id} seq:#{seq}>)
    end

    def handle_events(events)
      evolve(state, events)
      save
      [] # no commands
    end

    private

    attr_reader :backend, :logger

    def init_state(_id)
      nil
    end

    def save
      backend.transaction do
        run_sync_blocks(state, nil, [])
      end
    end

    class StateStored < self
      class << self
        def handle_events(events)
          instance = new(events.first.stream_id)
          instance.handle_events(events)
        end
      end
    end

    class EventSourced < self
      class << self
        def handle_events(events)
          # The current state already includes
          # the new events, so we need to load upto events.first.seq
          instance = load(events.first.stream_id, upto: events.first.seq - 1)
          instance.handle_events(events)
        end

        # Load from event history
        #
        # @param stream_id [String] the stream id
        # @return [Sourced::Projector::EventSourced]
        def load(stream_id, upto: nil)
          new(stream_id).load(upto:)
        end
      end

      # TODO: this is also in Decider. DRY up?
      def load(after: nil, upto: nil)
        events = backend.read_event_stream(id, after:, upto:)
        if events.any?
          @seq = events.last.seq 
          evolve(state, events)
        end
        self
      end
    end
  end
end
