# frozen_string_literal: true

module Sourced
  # Fills in a reactor's optional protocol methods with defaults so the Router
  # can call them unconditionally (no +respond_to?+ guards, no wrapper).
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

    # Default class-method implementations. +self+ inside each is the reactor
    # class (they are installed as singleton methods on the reactor).
    module Defaults
      # Consumer group id defaults to the reactor's class name (mirrors
      # Sourced::Consumer#group_id).
      def group_id = name

      # No partition keys declared. The Router treats an empty set as an
      # id-partitioned queue for exclusive reactors, and an error otherwise —
      # absence of keys never implies deletion.
      def partition_keys = []

      # Not the exclusive owner of its message types (routing concern only).
      def exclusive? = false

      # Delegate processing errors to the configured error strategy.
      def on_exception(exception, message, group)
        Sourced.config.error_strategy.call(exception, message, group)
      end

      def on_stop(_message = nil) = nil
      def on_start = nil
      def on_reset = nil

      # No history query conditions.
      def context_for(_attrs) = []
    end
  end
end
