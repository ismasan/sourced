# frozen_string_literal: true

module Sourced
  # Fills in a reactor's optional protocol methods with defaults so the Router
  # can call them unconditionally (no +respond_to?+ guards, no wrapper).
  # +on_exception+ is the exception: its fallback is the router's own error
  # strategy, so the Router checks for it (see Router#handle_exception).
  #
  # Rather than wrapping the reactor in a delegator, this defines the missing
  # methods directly on the reactor class — and only the ones it doesn't already
  # define, so the reactor's own definitions always win. Because the reactor
  # stays a real class, method-signature reflection ({Injector} detecting
  # +history:+ on +handle_claim+) keeps working.
  module ReactorDefaults
    module_function

    # Define any missing optional protocol methods on +reactor+. Idempotent.
    #
    # @param reactor [Class] a reactor class
    # @return [Class] the same reactor
    def apply(reactor)
      Defaults.instance_methods(false).each do |name|
        next if reactor.respond_to?(name)

        reactor.define_singleton_method(name, Defaults.instance_method(name))
      end
      reactor
    end

    # The reactors +reactor+ stands for: what its optional
    # +to_sourced_reactors+ returns, recursively (a machine returns itself and
    # the children it starts), or else itself. Each appears once.
    #
    # @param reactor [Class, #to_sourced_reactors]
    # @return [Array<Class>]
    def expand(reactor, seen = Set.new)
      return [] unless seen.add?(reactor)
      return [reactor] unless reactor.respond_to?(:to_sourced_reactors)

      reactor.to_sourced_reactors.flat_map { |r| r.equal?(reactor) ? [r] : expand(r, seen) }
    end

    # Default class-method implementations. +self+ inside each is the reactor
    # class (they are installed as singleton methods on the reactor).
    module Defaults
      # Consumer group id defaults to the reactor's class name, normalized by
      # {Types::GroupId} (mirrors Sourced::Consumer#group_id).
      def group_id = name && Types::GroupId.parse(name)

      # The name the group is shown under: the class name (mirrors
      # Sourced::Consumer#group_label).
      def group_label = name || group_id

      # No partition keys declared. The Router treats an empty set as an
      # id-partitioned queue for exclusive reactors, and an error otherwise —
      # absence of keys never implies deletion.
      def partition_keys = []

      # Not the exclusive owner of its message types (routing concern only).
      def exclusive? = false

      def on_stop(_message = nil) = nil
      def on_start = nil
      def on_reset = nil

      # No history query conditions.
      def context_for(_attrs) = []
    end
  end
end
