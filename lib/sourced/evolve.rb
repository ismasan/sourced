# frozen_string_literal: true

module Sourced
  # What a reactor reads its history through: a {Selector} (its context is the
  # messages it selects) or a {Projector::Inline} subclass. See {Evolve}.
  SourceInterface = Types::Interface[:selector, :to_projector]

  # Evolve mixin for reactors: evolves reactor context through its
  # {ClassMethods#source source}.
  #
  # By default the source is the reactor's own inline projector, declared by
  # the +context+ / +evolve+ DSL (blocks run in the projection's scope). A
  # reactor can instead adopt a standalone source with <tt>evolve SomeSource</tt>:
  # a projector or a {Selector}. An adopted projector is used as it is, so the
  # reactor declares no +context+ / +evolve+ / +before_evolve+ of its own; to
  # extend one, subclass it and adopt the subclass.
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
  #
  # @example Adopting an extended projector
  #   class ClosableCartProjector < CartProjector
  #     evolve CartClosed do |context, _evt|
  #       context[:closed] = true
  #     end
  #   end
  #
  #   class Carts < Sourced::Decider
  #     evolve ClosableCartProjector
  #   end
  #
  # @example Reading through a selector: the context is the messages
  #   class Carts < Sourced::Decider
  #     partition_by :cart_id
  #     evolve Sourced::Selector[CartStarted, CartClosed]
  #   end
  module Evolve
    def self.included(base)
      super
      # Prepended so these win over Consumer's default +history_selector+, whichever
      # order the reactor includes Evolve and extends Consumer in.
      base.singleton_class.prepend ClassMethods
    end

    # @return [Sourced::Projector::Inline, Selector::Projection] this reactor's
    #   projection, built on first access
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
        # A parent's own projector is complete by now, so the subclass extends a
        # subclass of it; an adopted source is shared as it is.
        if @source
          subclass.send(:__use_source, @source, projector)
        else
          subclass.send(:__extend_projector, projector)
        end
      end

      # @return [SourceInterface] the adopted source, else its own {#projector}
      def source = @source || projector

      # The adopted source's +to_projector+, as it is; else the reactor's own
      # subclass of {Sourced::Projector::Inline}, or of its parent reactor's
      # projector.
      #
      # @return [Class<Sourced::Projector::Inline>, Class<Selector::Projection>]
      def projector
        @projector ||= __build_projector(Sourced::Projector::Inline)
      end

      # What the reactor evolves from, without its partition.
      #
      # @return [Selector]
      def evolve_selector = projector.selector

      # The history the Router reads for a claimed partition: {#evolve_selector}
      # with its keyless parts scoped by the partition keys; keyed parts stay as
      # declared. {Selector::NONE} for an id-partitioned reactor.
      #
      # @return [Selector]
      def history_selector
        return Selector::NONE if partition_keys.empty? || claim_selector.id_partitioned?

        evolve_selector.by_default(*partition_keys)
      end

      # Define the projector's initial context factory. The block takes no arguments.
      def context(&)
        __own_projection!(:context)
        projector.context(&)
      end

      # Register a block on the reactor's projection to run before the evolve
      # handler of every message it handles. See {Sourced::Projector::Inline.before_evolve}.
      def before_evolve(&)
        __own_projection!(:before_evolve)
        projector.before_evolve(&)
      end

      # Register an evolve handler on the reactor's projection, or adopt a
      # standalone source.
      #
      # An adopted source is used as it is: the reactor takes no +context+ /
      # +evolve+ / +before_evolve+ declarations of its own besides it. Adopting
      # replaces an inherited source.
      #
      # @overload evolve(message_class, &block)
      #   @param message_class [Class] a {Sourced::Message} subclass
      #   @yield [context, message] runs in the projection's scope
      #   @raise [ArgumentError] when the reactor adopts a source
      # @overload evolve(source)
      #   @param source [SourceInterface] an inline projector, or a {Selector}
      #   @raise [ArgumentError] after the reactor's own declarations, or with a block
      def evolve(declared, &block)
        if SourceInterface === declared
          __adopt!(declared, block)
        else
          __own_projection!(:evolve)
          projector.evolve(declared, &block)
        end
      end

      private

      def __adopt!(source, block)
        raise ArgumentError, "#{self}.evolve(#{source.inspect}) takes no block" if block

        if @declares_projection
          raise ArgumentError, "#{self}.evolve(#{source.inspect}): #{self} declares context/evolve/before_evolve " \
                               'of its own, so it adopts no source'
        end
        @declares_projection = true
        __use_source(source)
      end

      def __use_source(source, projector = source.to_projector)
        @source = source
        @projector = projector
      end

      def __extend_projector(parent)
        @projector = __build_projector(parent)
      end

      def __own_projection!(dsl)
        if @source
          raise ArgumentError, "#{self}.#{dsl}: #{self} evolves #{@source.inspect}, which it uses as it is. " \
                               "To extend a projector, subclass it and adopt the subclass"
        end
        @declares_projection = true
      end

      def __build_projector(parent)
        reactor = self
        label = parent.name.nil? || parent == Sourced::Projector::Inline ? '' : "(#{parent.name})"
        Class.new(parent) do
          define_singleton_method(:to_s) { "#{reactor}::Projector#{label}" }
          define_singleton_method(:inspect) { to_s }
        end
      end
    end
  end
end
