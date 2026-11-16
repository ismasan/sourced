# frozen_string_literal: true

module Sourced
  class Projector
    # Tracks the newest store position among the messages a projection
    # evolves, so a reactor can tell whether a message is the latest thing its
    # projection has seen (ex. a reaction that should only act while its
    # trigger is current). Included by {Inline} and {Selector::Projection};
    # each calls {#track_position} for every message it evolves.
    #
    # Only messages the projection evolves count (its handled types), and only
    # positioned ones (read from a store): messages built in memory, ex. by the
    # GWT helpers, have no position and are not tracked.
    #
    # Within one claimed batch, Deciders evolve the batch's messages one at a
    # time, after the prior history, so an earlier message of the batch can't
    # see a later one.
    #
    # @example
    #   reaction ExpenseFiled => RecordUsdAmount | nil do |context, filed|
    #     next unless projection.current?(filed) # voided or filed again since
    #     ...
    #   end
    module Positions
      # @return [Integer, nil] the newest position evolved, nil before any
      attr_reader :last_position

      # Whether nothing this projection evolved came after +message+. A message
      # without a position (not read from a store) is current.
      #
      # @param message [Sourced::Message]
      # @return [Boolean]
      def current?(message)
        return true unless last_position && message.respond_to?(:position)

        message.position >= last_position
      end

      private

      def track_position(message)
        return unless message.respond_to?(:position)

        @last_position = message.position if @last_position.nil? || message.position > @last_position
      end
    end
  end
end
