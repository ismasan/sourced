# frozen_string_literal: true

module Sourced
  # Reactor base class for Sourced read-model projectors.
  class Projector
    include Sourced::Evolve
    include Sourced::React
    include Sourced::Sync
    extend Sourced::Consumer

    class << self
      # Projectors claim events they evolve from + events they react to.
      #
      # @return [Array<Class>] evolved and reacted-to message classes
      def handled_messages
        (handled_messages_for_evolve + handled_messages_for_react).uniq
      end

      private

      # Reactions are gated per message by {React#should_react?}, which by
      # default skips them while replaying. +sync+ / +after_sync+ blocks are
      # not gated: they run on every claim and receive +replaying:+ so they
      # can branch themselves.
      def build_action_pairs(instance, messages, replaying:)
        sync_actions = instance.collect_actions(
          state: instance.state, messages:, replaying:
        )

        reaction_pairs = each_with_partial_ack(messages) do |msg|
          next unless instance.reacts_to?(msg)
          next unless instance.should_react?(instance.state, msg, replaying:)

          reaction_msgs = Array(instance.react(msg))
          actions = Actions.build_for(reaction_msgs)
          actions.any? ? [actions, msg] : nil
        end

        reaction_pairs + [[sync_actions, messages.last]]
      end
    end

    attr_reader :partition_values

    # @param partition_values [Hash{Symbol => String}] partition key-value pairs
    def initialize(partition_values = {})
      @partition_values = partition_values
    end

    # Projector variant that evolves only the claimed messages on top of stored state.
    #
    # The stored state is loaded by the +load_state+ block, which runs in the
    # reactor's scope and so may read a database. When it returns +nil+ (or
    # isn't declared), the projection's +state+ block builds the initial state.
    #
    # @example
    #   class CartListing < Sourced::Projector::StateStored
    #     partition_by :cart_id
    #     evolve CartProjection
    #
    #     load_state { |partition_values| MyDB.find(partition_values[:cart_id]) }
    #     sync { |state:, **| MyDB.upsert(state) }
    #   end
    class StateStored < self
      class << self
        # Declare how to load the stored state for a partition.
        #
        # @yield [partition_values] runs in the reactor instance's scope
        # @yieldreturn [Object, nil] the stored state, or +nil+ if there is none
        def load_state(&)
          define_method(:load_state, &)
        end

        def handle_batch(partition_values, new_messages, history: nil, replaying: false)
          instance = new(partition_values)
          instance.evolve(new_messages)
          build_action_pairs(instance, new_messages, replaying:)
        end

        # @param claim [ClaimResult] claimed partition batch
        # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
        def handle_claim(claim)
          values = partition_keys.to_h { |k| [k, claim.partition_value[k.to_s]] }
          handle_batch(values, claim.messages, replaying: claim.replaying)
        end
      end

      # @param _partition_values [Hash{Symbol => String}] partition key-value pairs
      # @return [Object, nil] the stored state; +nil+ by default
      def load_state(_partition_values)
        nil
      end

      # The projection starts from the stored state when +load_state+ returns one.
      #
      # @return [Sourced::Projection]
      def projection
        @projection ||= begin
          stored = load_state(partition_values)
          stored.nil? ? self.class.projection_class.new : self.class.projection_class.new(stored)
        end
      end
    end

    # Projector variant that rebuilds state from full history each time.
    class EventSourced < self
      class << self
        def handle_batch(partition_values, new_messages, history:, replaying: false)
          instance = new(partition_values)
          instance.evolve(history.messages)
          build_action_pairs(instance, new_messages, replaying:)
        end

        # @param claim [ClaimResult] claimed partition batch
        # @param history [ReadResult] full partition history
        # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
        def handle_claim(claim, history:)
          values = partition_keys.to_h { |k| [k, claim.partition_value[k.to_s]] }
          handle_batch(values, claim.messages, history:, replaying: claim.replaying)
        end
      end
    end
  end
end
