# frozen_string_literal: true

module Sourced
  class Projector
    # Defines how a projection, an in-memory context, is evolved from messages.
    # An inline projector (a subclass) is the definition; a projection (an
    # instance) is its result. It knows nothing about stores, partitions or side
    # effects; evolve blocks run in the projection's scope, and identifying
    # values (ex. a cart's id) come from the messages. For the reactor that
    # persists projections, see {Sourced::Projector}.
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

        # @return [Selector] the evolved message types, without keys
        def selector
          @selector ||= Selector[*evolve_handlers.keys]
        end

        # {Sourced.load} protocol: {.selector} scoped by +partition+.
        def history_for(partition) = selector.by(**partition)

        # {Sourced.load} protocol: all +values+, as it declares no partition keys.
        def partition_of(values) = values

        # {Sourced.load} protocol: a fresh projection.
        def for_partition(_values) = new

        # {SourceInterface}: an inline projector is its own projector.
        def to_projector = self

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

        # Register a block to run before the evolve handler of every handled
        # message, in declaration order. Subclasses inherit them.
        #
        # @yield [context, message]
        # @raise [ArgumentError] without a block
        def before_evolve(&block)
          raise ArgumentError, "#{self}.before_evolve requires a block" unless block

          before_evolve_blocks << block
        end

        # @param declared [Class, #to_message_class]
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

      # Handlers are looked up by +msg.to_message.class+, so wrapped messages
      # dispatch too; they receive +msg+ as given. Unhandled messages are skipped.
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
