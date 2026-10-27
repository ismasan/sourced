# frozen_string_literal: true

module Sourced
  # Evolve mixin for reactors: evolves reactor context through a {Sourced::Projector}.
  #
  # The +context+ / +evolve+ DSL declares the reactor's projector, so evolve
  # blocks run in the projection's scope, not the reactor's. A reactor can also
  # adopt a standalone projector with <tt>evolve SomeProjector</tt>.
  #
  # Each reactor works on a projector subclass of its own (inheriting from the
  # adopted projector, or from its parent reactor's), so inline +evolve+ blocks
  # never change a projector another reactor uses.
  #
  # @example Inline projector
  #   class Carts < Sourced::Decider
  #     context { { items: [] } }
  #
  #     evolve ItemAdded do |context, evt|
  #       context[:items] << evt.payload.item_id
  #     end
  #   end
  #
  # @example Adopting a standalone projector
  #   class Carts < Sourced::Decider
  #     evolve CartProjector
  #   end
  module Evolve
    def self.included(base)
      super
      # Prepended so these win over Consumer's default +selector+, whichever
      # order the reactor includes Evolve and extends Consumer in.
      base.singleton_class.prepend ClassMethods
    end

    # @return [Sourced::Projector] this reactor's projection, built on first access
    def projection
      @projection ||= self.class.projector.new
    end

    # @return [Object] the projection's current context
    def context
      projection.context
    end

    # Apply messages to context through the projection.
    #
    # @param messages [Sourced::Message, Array<Sourced::Message>]
    # @return [Object] the evolved context
    def evolve(messages)
      projection.evolve(messages)
    end

    module ClassMethods
      def inherited(subclass)
        super
        subclass.send(:__use_projector, projector)
      end

      # @return [Class<Sourced::Projector>] the projector subclass this reactor owns
      def projector
        @projector ||= __build_projector(Sourced::Projector)
      end

      # The history the Router reads for a claimed partition: the projector's
      # selector scoped by the partition keys. {Selector::NONE} (no history)
      # for an id-partitioned reactor, which declares no partition keys or
      # +partition_by :__id+.
      #
      # @return [Selector]
      def selector
        keys = partition_keys
        return Selector::NONE if keys.empty? || IdPartition.keys?(keys)

        projector.selector.by(*keys)
      end

      # Define the projector's initial context factory. The block takes no arguments.
      def context(&)
        @declares_projection = true
        projector.context(&)
      end

      # Register a block on the reactor's projection to run before the evolve
      # handler of every message it handles. See {Sourced::Projector.before_evolve}.
      def before_evolve(&)
        @declares_projection = true
        projector.before_evolve(&)
      end

      # Register an evolve handler on the reactor's projection, or adopt a
      # standalone projector.
      #
      # Adopting must come before this reactor's own +context+ / +evolve+ /
      # +before_evolve+ declarations, which it would otherwise discard. It replaces handlers
      # inherited from a parent reactor.
      #
      # @overload evolve(message_class, &block)
      #   @param message_class [Class] a {Sourced::Message} subclass
      #   @yield [context, message] runs in the projection's scope
      # @overload evolve(projector)
      #   @param projector [Class<Sourced::Projector>] projector to adopt
      # @raise [ArgumentError] when adopting after own declarations, or with a block
      def evolve(klass, &block)
        if klass.is_a?(Class) && klass <= Sourced::Projector
          raise ArgumentError, "#{self}.evolve(#{klass}) takes no block" if block
          if @declares_projection
            raise ArgumentError,
                  "#{self}.evolve(#{klass}) must come before #{self}'s own context/evolve/before_evolve declarations"
          end

          @declares_projection = true
          @projector = __build_projector(klass)
        else
          @declares_projection = true
          projector.evolve(klass, &block)
        end
      end

      private

      def __use_projector(parent)
        @projector = __build_projector(parent)
      end

      def __build_projector(parent)
        reactor = self
        label = parent == Sourced::Projector || parent.name.nil? ? '' : "(#{parent.name})"
        Class.new(parent) do
          define_singleton_method(:to_s) { "#{reactor}::Projector#{label}" }
          define_singleton_method(:inspect) { to_s }
        end
      end
    end
  end
end
