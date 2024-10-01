# frozen_string_literal: true

module Sors
  module React
    PREFIX = 'reaction'

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def handled_reactions = self.class.handled_reactions

    def react(events)
      events.flat_map { |event| __handle_reaction(event) }
    end

    def __handle_reaction(event)
      method_name = Sors.message_method_name(PREFIX, event.class.name)
      return [] unless respond_to?(method_name)

      cmds = send(method_name, event)
      [cmds].flatten.compact
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_reactions.each do |evt_type|
          subclass.handled_reactions << evt_type
        end
      end

      def handled_reactions
        @handled_reactions ||= []
      end

      def react(event_type, &block)
        handled_reactions << event_type
        define_method(Sors.message_method_name(PREFIX, event_type.name), &block) if block_given?
      end
    end
  end
end
