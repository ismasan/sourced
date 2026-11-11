# frozen_string_literal: true

module Sourced
  class Projector
    # Defines how a projection, an in-memory context, is evolved from a history
    # of messages. An inline projector (a subclass) is the definition; a
    # projection (an instance) is its result. Inline: it runs in memory, where
    # it's called. For a reactor that persists projections, see {Sourced::Projector}.
    #
    # An inline projector knows nothing about stores, partitions, commands or side
    # effects: it declares an initial context and how each message type changes it,
    # so a projection's context is a function of the messages it evolves. Evolve
    # blocks run in the projection's own scope.
    #
    # Identifying values (ex. a cart's id) come from the messages, not from
    # outside: set them in the evolve blocks.
    #
    # Reactors evolve their context through an inline projector (see
    # {Sourced::Evolve}): their own, or a standalone one shared between reactors.
    #
    # @example
    #   class CartProjector < Sourced::Projector::Inline
    #     context do
    #       { cart_id: nil, items: [] }
    #     end
    #
    #     evolve ItemAdded do |context, evt|
    #       context[:cart_id] = evt.payload.cart_id
    #       context[:items] << evt.payload.item_id
    #     end
    #   end
    #
    #   projection = CartProjector.new
    #   projection.evolve(messages)
    #   projection.context # => { items: [...] }
    class Inline
      PREFIX = 'sourced_evolution'

      # Default for the +context+ argument, distinct from an explicit +nil+ context.
      UNSET = Object.new.freeze

      class << self
        def inherited(subclass)
          super
          subclass.evolve_handlers.merge!(evolve_handlers)
          subclass.before_evolve_blocks.concat(before_evolve_blocks)
        end

        # @return [Hash{Class => Symbol}] each handled message class's evolve method
        def evolve_handlers
          @evolve_handlers ||= {}
        end

        # What this projector evolves from, built by {.evolve}: a {Selector} of
        # the message types it has handlers for, without keys. Reactors scope it
        # by their partition keys; {Sourced.load} by the values it's given.
        #
        # @return [Selector]
        def selector
          @selector ||= Selector[*evolve_handlers.keys]
        end

        # What {Sourced.load} reads for +partition+: its {.selector} scoped by
        # it.
        #
        # @param partition [Hash{Symbol => Object}]
        # @return [Selector]
        def history_for(partition) = selector.by(**partition)

        # The partition among +values+, as {Sourced.load} reads it: all of
        # them, since an inline projector declares no partition keys.
        #
        # @param values [Hash{Symbol => Object}]
        # @return [Hash{Symbol => Object}]
        def partition_of(values) = values

        # A projection for the partition {Sourced.load} reads, starting from the
        # +context+ block's context.
        #
        # @return [Projector::Inline]
        def for_partition(_values) = new

        # A source's projector (see {SourceInterface}): an inline projector is its own.
        #
        # @return [self]
        def to_projector = self

        # Define the initial context factory.
        #
        # @yieldreturn [Object] the initial context
        # @raise [ArgumentError] if the block declares arguments: it receives none
        def context(&blk)
          if blk.arity.positive?
            raise ArgumentError,
                  "#{self}.context takes a block with no arguments; " \
                  'set identifying values (ex. ids) from messages in evolve blocks'
          end

          define_method(:init_context, &blk)
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
        #   before_evolve do |context, evt|
        #     context[:order_id] ||= evt.payload.order_id
        #     context[:updated_at] = evt.created_at
        #   end
        #
        # @yield [context, message]
        # @raise [ArgumentError] without a block
        def before_evolve(&block)
          raise ArgumentError, "#{self}.before_evolve requires a block" unless block

          before_evolve_blocks << block
        end

        # Register an evolve handler for a {Sourced::Message} subclass, or for the
        # class an object's +to_message_class+ returns (see {Sourced::Message.to_message_class}).
        #
        # @param declared [Class, #to_message_class] message class to evolve from
        # @yield [context, message] mutates +context+ for +message+
        def evolve(declared, &block)
          message_class = declared.to_message_class
          @selector = nil unless evolve_handlers.key?(message_class)
          evolve_handlers[message_class] = define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block)
        end
      end

      # @param context [Object] starting context, used instead of the +context+ block
      def initialize(context = UNSET)
        @context = context unless UNSET.equal?(context)
      end

      def init_context
        nil
      end

      # @return [Object] current context, built by the +context+ block on first access
      def context
        return @context if defined?(@context)

        @context = init_context
      end

      # Apply messages to context via registered handlers, each preceded by the
      # +before_evolve+ blocks. Handlers are looked up by +msg.to_message.class+,
      # so wrapped messages dispatch too; they receive +msg+ as given. Skips
      # messages without a registered handler.
      #
      # @param messages [Sourced::Message, Array<Sourced::Message>]
      # @return [Object] the evolved context
      def evolve(messages)
        handlers = self.class.evolve_handlers
        Array(messages).each do |msg|
          method_name = handlers[msg.to_message.class]
          next unless method_name

          self.class.before_evolve_blocks.each { |blk| instance_exec(context, msg, &blk) }
          send(method_name, context, msg)
        end
        context
      end
    end
  end
end
