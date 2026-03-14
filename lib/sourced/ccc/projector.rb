# frozen_string_literal: true

module Sourced
  module CCC
    # Reactor base class for CCC read-model projectors.
    class Projector
      include CCC::Evolve
      include CCC::React
      include CCC::Sync
      extend CCC::Consumer

      class << self
        # Projectors claim events they evolve from + events they react to.
        #
        # @return [Array<Class>] evolved and reacted-to message classes
        def handled_messages
          (handled_messages_for_evolve + handled_messages_for_react).uniq
        end

        private

        def build_action_pairs(instance, messages, replaying:)
          sync_actions = instance.collect_actions(
            state: instance.state, messages: messages, replaying: replaying
          )

          reaction_pairs = if replaying
            []
          else
            each_with_partial_ack(messages) do |msg|
              next unless instance.reacts_to?(msg)
              reaction_msgs = Array(instance.react(msg))
              actions = Actions.build_for(reaction_msgs)
              actions.any? ? [actions, msg] : nil
            end
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
      class StateStored < self
        class << self
          def handle_batch(partition_values, new_messages, replaying: false)
            instance = new(partition_values)
            instance.evolve(new_messages)
            build_action_pairs(instance, new_messages, replaying: replaying)
          end

          # @param claim [ClaimResult] claimed partition batch
          # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
          def handle_claim(claim)
            values = partition_keys.to_h { |k| [k, claim.partition_value[k.to_s]] }
            handle_batch(values, claim.messages, replaying: claim.replaying)
          end
        end
      end

      # Projector variant that rebuilds state from full history each time.
      class EventSourced < self
        class << self
          def handle_batch(partition_values, new_messages, history:, replaying: false)
            instance = new(partition_values)
            instance.evolve(history.messages)
            build_action_pairs(instance, new_messages, replaying: replaying)
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
end
