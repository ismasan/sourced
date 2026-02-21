# frozen_string_literal: true

module Sourced
  module CCC
    class Projector
      include CCC::Evolve
      include CCC::React
      include CCC::Sync
      extend CCC::Consumer

      class << self
        # Projectors claim events they evolve from + events they react to.
        def handled_messages
          (handled_messages_for_evolve + handled_messages_for_react).uniq
        end

        # No history: — uses claim.messages directly.
        def handle_batch(claim)
          values = partition_keys.map { |k| claim.partition_value[k.to_s] }
          instance = new(values)
          instance.evolve(claim.messages)

          sync_actions = instance.sync_actions(
            state: instance.state, messages: claim.messages, replaying: claim.replaying
          )

          reaction_pairs = if claim.replaying
            []
          else
            each_with_partial_ack(claim.messages) do |msg|
              next unless instance.reacts_to?(msg)
              reaction_msgs = Array(instance.react(msg))
              reaction_msgs.any? ? [Actions::Append.new(reaction_msgs), msg] : nil
            end
          end

          reaction_pairs + [[sync_actions, claim.messages.last]]
        end
      end

      attr_reader :partition_values

      def initialize(partition_values = [])
        @partition_values = partition_values
      end
    end
  end
end
