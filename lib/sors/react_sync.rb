# frozen_string_literal: true

module Sors
  module ReactSync
    PREFIX = 'syncreaction'

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def handled_sync_reactions = self.class.handled_sync_reactions

    def react_sync(state, events)
      events.flat_map { |event| __handle_sync_reaction(state, event) }
    end

    def __handle_sync_reaction(state, event)
      method_name = Sors.message_method_name(PREFIX, event.class.name)
      return [] unless respond_to?(method_name)

      cmds = send(method_name, state, event)
      [cmds].flatten.compact
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_sync_reactions.each do |evt_type|
          subclass.handled_sync_reactions << evt_type
        end
      end

      def handled_sync_reactions
        @handled_sync_reactions ||= []
      end

      def react_sync(event_type, &block)
        handled_sync_reactions << event_type
        define_method(Sors.message_method_name(PREFIX, event_type.name), &block) if block_given?
      end
    end
  end
end
