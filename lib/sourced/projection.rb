# frozen_string_literal: true

module Sourced
  # In-memory state evolved from a history of messages.
  #
  # A projection knows nothing about stores, partitions, commands or side
  # effects: it declares an initial state and how each message type changes it,
  # so its state is a function of the messages it evolves. Evolve blocks run in
  # the projection's own scope.
  #
  # Identifying values (ex. a cart's id) come from the messages, not from
  # outside: set them in the evolve blocks.
  #
  # Reactors evolve their state through a projection class (see {Sourced::Evolve}),
  # either one declared inline or a standalone class shared between reactors.
  #
  # @example
  #   class CartProjection < Sourced::Projection
  #     state do
  #       { cart_id: nil, items: [] }
  #     end
  #
  #     evolve ItemAdded do |state, evt|
  #       state[:cart_id] = evt.payload.cart_id
  #       state[:items] << evt.payload.item_id
  #     end
  #   end
  #
  #   projection = CartProjection.new
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
        subclass.before_evolve_blocks.concat(before_evolve_blocks)
      end

      # @return [Array<Class>] message classes this projection evolves from
      def handled_messages
        @handled_messages ||= []
      end

      # The name reactors use for the types they evolve from, so a projection
      # can be loaded the same way (see {Sourced.load}).
      alias handled_messages_for_evolve handled_messages

      # Define the initial state factory.
      #
      # @yieldreturn [Object] the initial state
      # @raise [ArgumentError] if the block declares arguments: it receives none
      def state(&blk)
        if blk.arity.positive?
          raise ArgumentError,
                "#{self}.state takes a block with no arguments; " \
                'set identifying values (ex. ids) from messages in evolve blocks'
        end

        define_method(:init_state, &blk)
      end

      # @return [Array<Proc>] blocks run before each evolve handler, in order
      def before_evolve_blocks
        @before_evolve_blocks ||= []
      end

      # Register a block to run before the evolve handler of every message this
      # projection handles, in the projection's scope. Messages without a
      # handler are still skipped. Blocks run in declaration order, and
      # subclasses inherit them.
      #
      # @example Stamp every order event
      #   before_evolve do |state, evt|
      #     state[:order_id] ||= evt.payload.order_id
      #     state[:updated_at] = evt.created_at
      #   end
      #
      # @yield [state, message]
      # @raise [ArgumentError] without a block
      def before_evolve(&block)
        raise ArgumentError, "#{self}.before_evolve requires a block" unless block

        before_evolve_blocks << block
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

    # @param state [Object] starting state, used instead of the +state+ block
    def initialize(state = UNSET)
      @state = state unless UNSET.equal?(state)
    end

    def init_state
      nil
    end

    # @return [Object] current state, built by the +state+ block on first access
    def state
      return @state if defined?(@state)

      @state = init_state
    end

    # Apply messages to state via registered handlers, each preceded by the
    # +before_evolve+ blocks. Skips messages without a registered handler.
    #
    # @param messages [Sourced::Message, Array<Sourced::Message>]
    # @return [Object] the evolved state
    def evolve(messages)
      Array(messages).each do |msg|
        method_name = Sourced.message_method_name(PREFIX, msg.class.to_s)
        next unless respond_to?(method_name)

        self.class.before_evolve_blocks.each { |blk| instance_exec(state, msg, &blk) }
        send(method_name, state, msg)
      end
      state
    end
  end
end
