# frozen_string_literal: true

module Sourced
  # What a reactor reads its history through: a {Selector} (its context is the
  # messages it selects) or a {Projector::Inline} subclass (its context is the
  # projection's). It answers the selector the reactor reads, scoped to the
  # partition, and the projector the reactor evolves. See {Evolve}.
  SourceInterface = Types::Interface[:selector, :to_projector]

  # Evolve mixin for reactors: evolves reactor context through its
  # {ClassMethods#source source}.
  #
  # By default the source is a {Sourced::Projector::Inline} subclass of the
  # reactor's own, which the +context+ / +evolve+ DSL declares, so evolve
  # blocks run in the projection's scope, not the reactor's. A reactor can also
  # adopt a standalone source with <tt>evolve SomeSource</tt>: a projector, or
  # (for a Decider) a {Selector}.
  #
  # Each reactor works on a projector subclass of its own (of its source's
  # +to_projector+, or its parent reactor's projector), so inline +evolve+
  # blocks never change a projector another reactor uses. A selector's
  # projector takes no inline declarations.
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
  # @example Reading through a selector: the context is the messages
  #   class Carts < Sourced::Decider
  #     partition_by :cart_id
  #     evolve Sourced::Selector[CartStarted, CartClosed].excludes(CartClosed)
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

    # Whether the source's assertion holds over the context so far, in this
    # reactor's partition (+partition_values+): a selector source's
    # precondition. True for a source without one, ex. an inline projector.
    # Deciders and machines decide a command only while it holds.
    #
    # @return [Boolean]
    def source_holds?
      selector = self.class.__source_selector
      selector.assertion == Selector::Assertion::TRUE || selector.bind(**partition_values).holds?(context)
    end

    module ClassMethods
      def inherited(subclass)
        super
        subclass.send(:__use_source, @source, projector)
      end

      # What the reactor reads its history through: the source it adopted with
      # {#evolve}, else its own {#projector}.
      #
      # @return [SourceInterface]
      def source = @source || projector

      # The projector this reactor owns and its projections are built from: a
      # subclass of its source's +to_projector+ (an inline projector, or a
      # {Selector::Projection}), or of its parent reactor's.
      #
      # @return [Class<Sourced::Projector::Inline>, Class<Selector::Projection>]
      def projector
        @projector ||= __build_projector(Sourced::Projector::Inline)
      end

      # What the reactor evolves from, without its partition: its projector's
      # selector (a selector source's, as given). What it reacts to with a
      # catch-all +reaction+, and what Topology draws, however it's partitioned.
      #
      # @return [Selector]
      def evolve_selector = projector.selector

      # The history the Router reads for a claimed partition: {#evolve_selector}
      # with its parts that have no keys scoped by the partition keys
      # ({Selector#by_default}), and the others as declared (a selector
      # source's parts with keys of their own, which {PartitionSchema.check!}
      # holds to partition keys). {Selector::NONE} (no history) for an
      # id-partitioned reactor, which declares no partition keys or
      # +partition_by :__id+.
      #
      # @return [Selector]
      def history_selector
        return Selector::NONE if partition_keys.empty? || claim_selector.id_partitioned?

        evolve_selector.by_default(*partition_keys)
      end

      # Validate the definition, then fix {#history_selector} for
      # {#__source_selector}.
      def validate!
        super.tap { @__source_selector = history_selector }
      end

      # @api private
      # {#history_selector}, fixed by {#validate!}, read per call before it.
      # What {Evolve#source_holds?} checks.
      # @return [Selector]
      def __source_selector = @__source_selector || history_selector

      # Define the projector's initial context factory. The block takes no arguments.
      def context(&)
        @declares_projection = true
        projector.context(&)
      end

      # Register a block on the reactor's projection to run before the evolve
      # handler of every message it handles. See {Sourced::Projector::Inline.before_evolve}.
      def before_evolve(&)
        @declares_projection = true
        projector.before_evolve(&)
      end

      # Register an evolve handler on the reactor's projection, or adopt a
      # standalone source.
      #
      # Adopting must come before this reactor's own +context+ / +evolve+ /
      # +before_evolve+ declarations, which it would otherwise discard. It replaces
      # the source inherited from a parent reactor.
      #
      # @overload evolve(message_class, &block)
      #   @param message_class [Class] a {Sourced::Message} subclass
      #   @yield [context, message] runs in the projection's scope
      #   @raise [ArgumentError] for a selector source, which takes no handlers
      # @overload evolve(source)
      #   @param source [SourceInterface] an inline projector, or a {Selector}
      # @raise [ArgumentError] when adopting after own declarations, or with a
      #   block
      def evolve(declared, &block)
        if SourceInterface === declared
          __adopt!(declared, block)
        else
          @declares_projection = true
          projector.evolve(declared, &block)
        end
      end

      private

      def __adopt!(source, block)
        raise ArgumentError, "#{self}.evolve(#{source.inspect}) takes no block" if block

        if @declares_projection
          raise ArgumentError, "#{self}.evolve(#{source.inspect}) must come before #{self}'s own " \
                               'context/evolve/before_evolve declarations'
        end
        @declares_projection = true
        __use_source(source, source.to_projector)
      end

      def __use_source(source, parent)
        @source = source
        @projector = __build_projector(parent)
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
