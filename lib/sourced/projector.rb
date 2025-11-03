# frozen_string_literal: true

module Sourced
  # Projectors react to events
  # and update views of current state somewhere (a DB, files, etc)
  class Projector
    include React
    include Evolve
    include Sync
    extend Consumer

    REACTION_WITH_STATE_PREFIX = 'reaction_with_state'

    class << self
      # The Reactor interface
      def handled_messages = handled_messages_for_evolve

      def reaction(event_class = nil, &block)
        if event_class && !handled_messages_for_evolve.include?(event_class)
          raise ArgumentError, '.reaction only works with event types handled by this class via .event(event_type)' 
        end

        super
      end

      def handle(message, replaying:, history:)
        new(id: message.stream_id).handle(message, replaying:, history:)
      end
    end

    attr_reader :id, :seq

    def initialize(id:, logger: Sourced.config.logger)
      @id = id
      @seq = 0
      @logger = logger
    end

    def inspect
      %(<#{self.class} id:#{id} seq:#{seq}>)
    end

    def handle(message, replaying:)
      raise NotImplementedError, 'implement me in subclasses'
    end

    private

    attr_reader :logger

    # Override Evolve#__update_on_evolve
    def __update_on_evolve(event)
      @seq = event.seq
    end

    # A StateStored projector fetches initial state from
    # storage somewhere (DB, files, API)
    # And then after reacting to events and updating state,
    # it can save it back to the same or different storage.
    # @example
    #
    #  class CartListings < Sourced::Projector::StateStored
    #    # Fetch listing record from DB, or new one.
    #    state do |id|
    #      CartListing.find_or_initialize(id)
    #    end
    #
    #    # Evolve listing record from events
    #    evolve Carts::ItemAdded do |listing, event|
    #      listing.total += event.payload.price
    #    end
    #
    #    # Sync listing record back to DB
    #    sync do |state:, events:, replaying:|
    #      state.save!
    #    end
    #  end
    class StateStored < self
      class << self
        # State-stored version doesn't load :history
        def handle(message, replaying: false)
          new(id: message.stream_id).handle(message, replaying:)
        end
      end

      def handle(message, replaying:)
        # Load state from storage  
        state
        # Evolve new message
        evolve(message)
        # Collect sync actions
        actions = sync_actions_with(state:, events: [message], replaying:)
        # Replaying? Just return sync action
        return actions if replaying

        # Not replaying. Also run reactions
        if reacts_to?(message)
          actions += Actions.build_for(react(message))
        end

        actions
      end
    end

    # An EventSourced projector fetches initial state from
    # past events in the event store.
    # And then after reacting to events and updating state,
    # it can save it to a DB table, a file, etc.
    # @example
    #
    #  class CartListings < Sourced::Projector::EventSourced
    #    # Initial in-memory state
    #    state do |id|
    #      { id:, total: 0 }
    #    end
    #
    #    # Evolve listing record from events
    #    evolve Carts::ItemAdded do |listing, event|
    #      listing[:total] += event.payload.price
    #    end
    #
    #    # Sync listing record to a file
    #    sync do |state:, events:, replaying:|
    #      File.write("/listings/#{state[:id]}.json", JSON.dump(state)) 
    #    end
    #  end
    class EventSourced < self
      def handle(message, replaying:, history:)
        # Evolve new message from history
        evolve(history)
        # Collect sync actions
        actions = sync_actions_with(state:, events: [message], replaying:)
        # Replaying? Just return sync action
        return actions if replaying

        # Not replaying. Also run reactions
        if reacts_to?(message)
          actions += Actions.build_for(react(message))
        end

        actions
      end
    end
  end
end
