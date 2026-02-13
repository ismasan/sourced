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
    BLANK_ARRAY = [].freeze

    class << self
      # The Reactor interface
      def handled_messages = handled_messages_for_evolve

      # Override this check defined in React mixin
      private def __validate_message_for_reaction!(event_class)
        if event_class && !handled_messages_for_evolve.include?(event_class)
          raise ArgumentError, '.reaction only works with event types handled by this class via .event(event_type)' 
        end
      end

      # The Ractor Interface
      # @param message [Sourced::Message]
      # @option replaying [Boolean]
      # @option history [Enumerable<Sourced::Message>]
      # @return [Array<Sourced::Actions::*>]
      def handle(message, replaying:, history: BLANK_ARRAY)
        new(id: identity_from(message)).handle(message, replaying:, history:)
      end

      # Override this in subclasses 
      # to make an actor take it's @id from an arbitrary 
      # field in the message
      #
      # @param message [Sourced::Message]
      # @return [Object]
      def identity_from(message) = message.stream_id
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
          new(id: identity_from(message)).handle(message, replaying:)
        end

        # Optimized batch processing: one state load, one sync for entire batch.
        # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
        # @return [Array<[actions, source_message]>] action pairs
        def handle_batch(batch)
          first_msg, _ = batch.first
          instance = new(id: identity_from(first_msg))
          messages = batch.map(&:first)
          all_replaying = batch.all? { |_, r| r }

          # 1. Load state from storage
          instance.state
          # 2. Evolve ALL batch messages at once
          instance.evolve(messages)
          # 3. Single sync for entire batch
          sync_actions = instance.sync_actions_with(
            state: instance.state, events: messages, replaying: all_replaying
          )

          last_msg = messages.last
          result = [[sync_actions, last_msg]]

          # 4. Reactions (only for non-replaying messages)
          unless all_replaying
            batch.each do |msg, replaying|
              next if replaying
              if instance.reacts_to?(msg)
                reaction_cmds = instance.react(msg)
                result << [Actions.build_for(reaction_cmds), msg] if reaction_cmds.any?
              end
            end
          end

          result
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
      BLANK_HISTORY = [].freeze

      class << self
        # Optimized batch processing: one history evolve, one sync for entire batch.
        # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
        # @param history [Array<Message>] full stream history
        # @return [Array<[actions, source_message]>] action pairs
        def handle_batch(batch, history: BLANK_HISTORY)
          first_msg, _ = batch.first
          instance = new(id: identity_from(first_msg))
          messages = batch.map(&:first)
          all_replaying = batch.all? { |_, r| r }

          # 1. Evolve full history (includes batch messages)
          instance.evolve(history)
          # 2. Single sync
          sync_actions = instance.sync_actions_with(
            state: instance.state, events: messages, replaying: all_replaying
          )

          last_msg = messages.last
          result = [[sync_actions, last_msg]]

          # 3. Reactions (only for non-replaying messages)
          unless all_replaying
            batch.each do |msg, replaying|
              next if replaying
              if instance.reacts_to?(msg)
                reaction_cmds = instance.react(msg)
                result << [Actions.build_for(reaction_cmds), msg] if reaction_cmds.any?
              end
            end
          end

          result
        end
      end

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
