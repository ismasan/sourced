# frozen_string_literal: true

module Sourced
  class Projector
    include Evolve
    include Sync
    extend Consumer

    class << self
      def handled_events = handled_events_for_evolve
    end

    attr_reader :id, :state

    def initialize(id, backend: Sourced.config.backend, logger: Sourced.config.logger)
      @id = id
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
  end
end
