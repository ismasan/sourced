# frozen_string_literal: true

module Sourced
  # Evolve mixin for reactors: evolves reactor state through a {Sourced::Projection}.
  #
  # The +state+ / +evolve+ DSL declares the reactor's projection, so evolve
  # blocks run in the projection's scope, not the reactor's. A reactor can also
  # adopt a standalone projection class with <tt>evolve SomeProjection</tt>.
  #
  # Each reactor works on a projection subclass of its own (inheriting from the
  # adopted projection, or from its parent reactor's), so inline +evolve+ blocks
  # never change a projection class another reactor uses.
  #
  # @example Inline projection
  #   class Carts < Sourced::Decider
  #     state { { items: [] } }
  #
  #     evolve ItemAdded do |state, evt|
  #       state[:items] << evt.payload.item_id
  #     end
  #   end
  #
  # @example Adopting a standalone projection
  #   class Carts < Sourced::Decider
  #     evolve CartProjection
  #   end
  module Evolve
    def self.included(base)
      super
      # Prepended so these win over Consumer's default +handled_messages_for_evolve+,
      # whichever order the reactor includes Evolve and extends Consumer in.
      base.singleton_class.prepend ClassMethods
    end

    # @return [Sourced::Projection] this reactor's projection, built on first access
    def projection
      @projection ||= self.class.projection_class.new
    end

    # @return [Object] the projection's current state
    def state
      projection.state
    end

    # Apply messages to state through the projection.
    #
    # @param messages [Sourced::Message, Array<Sourced::Message>]
    # @return [Object] the evolved state
    def evolve(messages)
      projection.evolve(messages)
    end

    module ClassMethods
      def inherited(subclass)
        super
        subclass.send(:__use_projection_class, projection_class)
      end

      # @return [Class<Sourced::Projection>] the projection subclass this reactor owns
      def projection_class
        @projection_class ||= __build_projection_class(Sourced::Projection)
      end

      # @return [Array<Class>] message classes this reactor's projection evolves from
      def handled_messages_for_evolve
        projection_class.handled_messages
      end

      # Define the projection's initial state factory. The block takes no arguments.
      def state(&)
        @declares_projection = true
        projection_class.state(&)
      end

      # Register a block on the reactor's projection to run before the evolve
      # handler of every message it handles. See {Sourced::Projection.before_evolve}.
      def before_evolve(&)
        @declares_projection = true
        projection_class.before_evolve(&)
      end

      # Register an evolve handler on the reactor's projection, or adopt a
      # standalone projection class.
      #
      # Adopting must come before this reactor's own +state+ / +evolve+ /
      # +before_evolve+ declarations, which it would otherwise discard. It replaces handlers
      # inherited from a parent reactor.
      #
      # @overload evolve(message_class, &block)
      #   @param message_class [Class] a {Sourced::Message} subclass
      #   @yield [state, message] runs in the projection's scope
      # @overload evolve(projection_class)
      #   @param projection_class [Class<Sourced::Projection>] projection to adopt
      # @raise [ArgumentError] when adopting after own declarations, or with a block
      def evolve(klass, &block)
        if klass.is_a?(Class) && klass <= Sourced::Projection
          raise ArgumentError, "#{self}.evolve(#{klass}) takes no block" if block
          if @declares_projection
            raise ArgumentError,
                  "#{self}.evolve(#{klass}) must come before #{self}'s own state/evolve/before_evolve declarations"
          end

          @declares_projection = true
          @projection_class = __build_projection_class(klass)
        else
          @declares_projection = true
          projection_class.evolve(klass, &block)
        end
      end

      private

      def __use_projection_class(parent)
        @projection_class = __build_projection_class(parent)
      end

      def __build_projection_class(parent)
        reactor = self
        label = parent == Sourced::Projection || parent.name.nil? ? '' : "(#{parent.name})"
        Class.new(parent) do
          define_singleton_method(:to_s) { "#{reactor}::Projection#{label}" }
          define_singleton_method(:inspect) { to_s }
        end
      end
    end
  end
end
