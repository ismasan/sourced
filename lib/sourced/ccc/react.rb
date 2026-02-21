# frozen_string_literal: true

module Sourced
  module CCC
    # React mixin for CCC reactors.
    # Reactions return raw CCC::Message instances (not correlated).
    # The runtime handles auto-correlation via Actions::Append#execute.
    module React
      PREFIX = 'ccc_reaction'
      EMPTY_ARRAY = [].freeze

      def self.included(base)
        super
        base.extend ClassMethods
      end

      # Run the reaction handler for a single message.
      # Returns raw messages (not correlated).
      def react(message)
        method_name = Sourced.message_method_name(PREFIX, message.class.to_s)
        if respond_to?(method_name)
          Array(send(method_name, state, message)).compact
        else
          EMPTY_ARRAY
        end
      end

      def reacts_to?(message)
        self.class.handled_messages_for_react.include?(message.class)
      end

      module ClassMethods
        def inherited(subclass)
          super
          handled_messages_for_react.each do |klass|
            subclass.handled_messages_for_react << klass
          end
        end

        def handled_messages_for_react
          @handled_messages_for_react ||= []
        end

        # Register a reaction handler for a CCC::Message subclass.
        def reaction(message_class, &block)
          handled_messages_for_react << message_class
          define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block)
        end
      end
    end
  end
end
