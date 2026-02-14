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
      # to make an actor take its @id from an arbitrary
      # field in the message
      #
      # @param message [Sourced::Message]
      # @return [Object]
      def identity_from(message) = message.stream_id

      private

      # Shared batch finalization for both StateStored and EventSourced projectors.
      # Runs reaction handlers first (which may issue 3rd party requests),
      # then builds deferred sync actions last (executed in backend DB transaction).
      #
      # Unlike Actors (which create a new instance per message), Projectors use an
      # evolve-all-sync-once optimization: all batch messages are evolved into a
      # single instance's state before reactions run. This means on partial failure,
      # the sync must be rebuilt for only the successfully processed messages.
      # The block is called with the partial message list to produce correct sync_actions
      # (e.g. StateStored rebuilds a fresh instance with partial evolve).
      #
      # @param instance [Projector] the projector instance (already evolved)
      # @param batch [Array<[Message, Boolean]>] original batch pairs
      # @param messages [Array<Message>] extracted messages from batch
      # @param all_replaying [Boolean] whether all messages in the batch are replaying
      # @yield [partial_messages] called on reaction error to build partial sync_actions
      # @yieldparam partial_messages [Array<Message>] messages successfully processed
      # @yieldreturn [Array<Sourced::Actions::Sync>] sync actions for partial state
      # @return [Array<[actions, source_message]>] action pairs
      def sync_and_react(instance, batch, messages, all_replaying, &on_partial_sync)
        reaction_pairs = if all_replaying
          BLANK_ARRAY
        else
          each_with_partial_ack(batch) do |msg, replaying|
            next if replaying
            next unless instance.reacts_to?(msg)

            reaction_cmds = instance.react(msg)
            reaction_cmds.any? ? [Actions.build_for(reaction_cmds), msg] : nil
          end
        end

        # All reactions succeeded. Build deferred sync for all messages (runs last in backend transaction).
        sync_actions = instance.sync_actions_with(
          state: instance.state, events: messages, replaying: all_replaying
        )

        reaction_pairs + [[sync_actions, messages.last]]
      rescue PartialBatchError => e
        # Augment partial reaction results with sync for the successfully processed messages.
        fail_idx = messages.index { |m| m.id == e.failed_message.id }
        partial_messages = messages[0...fail_idx]
        if on_partial_sync && partial_messages.any?
          sync_actions = on_partial_sync.call(partial_messages)
          e.action_pairs << [sync_actions, partial_messages.last] if sync_actions.any?
        end
        raise
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
          new(id: identity_from(message)).handle(message, replaying:)
        end

        # Optimized batch processing: one state load, one sync for entire batch.
        # On partial failure, rebuilds a fresh instance with only the successfully
        # processed messages so the sync persists correct partial state.
        # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
        # @return [Array<[actions, source_message]>] action pairs
        def handle_batch(batch)
          first_msg, _ = batch.first
          instance = new(id: identity_from(first_msg))
          messages = batch.map(&:first)
          all_replaying = batch.all? { |_, r| r }

          instance.state
          instance.evolve(messages)
          sync_and_react(instance, batch, messages, all_replaying) do |partial_messages|
            partial = new(id: identity_from(first_msg))
            partial.state
            partial.evolve(partial_messages)
            partial.sync_actions_with(state: partial.state, events: partial_messages, replaying: all_replaying)
          end
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
        # On partial failure, reuses the already-evolved instance for sync since
        # EventSourced state is rebuilt from history on every invocation (idempotent).
        # @param batch [Array<[Message, Boolean]>] array of [message, replaying] pairs
        # @param history [Array<Message>] full stream history
        # @return [Array<[actions, source_message]>] action pairs
        def handle_batch(batch, history: BLANK_HISTORY)
          first_msg, _ = batch.first
          instance = new(id: identity_from(first_msg))
          messages = batch.map(&:first)
          all_replaying = batch.all? { |_, r| r }

          instance.evolve(history)
          sync_and_react(instance, batch, messages, all_replaying) do |partial_messages|
            # EventSourced state is rebuilt from history, so sync is idempotent.
            # Reuse the already-evolved instance but sync only partial_messages.
            instance.sync_actions_with(state: instance.state, events: partial_messages, replaying: all_replaying)
          end
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
