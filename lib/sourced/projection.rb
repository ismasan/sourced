# frozen_string_literal: true

module Sourced
  # In-memory state evolved from a history of messages.
  #
  # A projection knows nothing about stores, claims, commands or side effects:
  # it declares an initial state and how each message type changes it. Evolve
  # blocks run in the projection's own scope.
  #
  # Reactors evolve their state through a projection class (see {Sourced::Evolve}),
  # either one declared inline or a standalone class shared between reactors.
  #
  # @example
  #   class CartProjection < Sourced::Projection
  #     state do |_partition_values|
  #       { items: [] }
  #     end
  #
  #     evolve ItemAdded do |state, evt|
  #       state[:items] << evt.payload.item_id
  #     end
  #   end
  #
  #   projection = CartProjection.new(cart_id: 'c1')
  #   projection.evolve(messages)
  #   projection.state # => { items: [...] }
  class Projection
    PREFIX = 'sourced_evolution'

    # Default for the +state+ argument, distinct from an explicit +nil+ state.
    UNSET = Object.new.freeze

    class << self
      def inherited(subclass)
        super
        subclass.handled_messages.concat(handled_messages)
      end

      # @return [Array<Class>] message classes this projection evolves from
      def handled_messages
        @handled_messages ||= []
      end

      # Define the initial state factory. The block receives the partition values hash.
      def state(&blk)
        define_method(:init_state, &blk)
      end

      # Register an evolve handler for a {Sourced::Message} subclass.
      #
      # @param message_class [Class] message class to evolve from
      # @yield [state, message] mutates +state+ for +message+
      # @raise [ArgumentError] if +message_class+ isn't a {Sourced::Message} subclass
      def evolve(message_class, &block)
        unless message_class.is_a?(Class) && message_class < Sourced::Message
          raise ArgumentError, "#{self}.evolve expects a Sourced::Message subclass, got #{message_class.inspect}"
        end

        handled_messages << message_class unless handled_messages.include?(message_class)
        define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block)
      end
    end

    attr_reader :partition_values

    # @param partition_values [Hash{Symbol => String}] partition key-value pairs,
    #   passed to the +state+ block
    # @param state [Object] starting state, used instead of the +state+ block
    def initialize(partition_values = {}, state = UNSET)
      @partition_values = partition_values
      @state = state unless UNSET.equal?(state)
    end

    def init_state(_partition_values)
      nil
    end

    # @return [Object] current state, built by the +state+ block on first access
    def state
      return @state if defined?(@state)

      @state = init_state(partition_values)
    end

    # Apply messages to state via registered handlers.
    # Skips messages without a registered handler.
    #
    # @param messages [Sourced::Message, Array<Sourced::Message>]
    # @return [Object] the evolved state
    def evolve(messages)
      Array(messages).each do |msg|
        method_name = Sourced.message_method_name(PREFIX, msg.class.to_s)
        send(method_name, state, msg) if respond_to?(method_name)
      end
      state
    end
  end
end
