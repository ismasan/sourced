# frozen_string_literal: true

module Sourced
  module CCC
    # Evolve mixin for CCC reactors.
    # Adapted from Sourced::Evolve for CCC::Message (no stream_id/seq).
    # State block receives partition values array instead of stream id.
    module Evolve
      PREFIX = 'ccc_evolution'

      def self.included(base)
        super
        base.extend ClassMethods
      end

      def init_state(_partition_values)
        nil
      end

      def state
        @state ||= init_state(partition_values)
      end

      def partition_values
        @partition_values ||= []
      end

      # Apply messages to state via registered handlers.
      # Skips messages without a registered handler.
      def evolve(messages)
        Array(messages).each do |msg|
          method_name = Sourced.message_method_name(PREFIX, msg.class.to_s)
          send(method_name, state, msg) if respond_to?(method_name)
        end
        state
      end

      module ClassMethods
        def inherited(subclass)
          super
          handled_messages_for_evolve.each do |klass|
            subclass.handled_messages_for_evolve << klass
          end
        end

        def handled_messages_for_evolve
          @handled_messages_for_evolve ||= []
        end

        # Define initial state factory. Block receives partition values array.
        def state(&blk)
          define_method(:init_state, &blk)
        end

        # Register an evolve handler for a CCC::Message subclass.
        def evolve(message_class, &block)
          handled_messages_for_evolve << message_class
          define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block)
        end
      end
    end
  end
end
