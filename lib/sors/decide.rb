# frozen_string_literal: true

module Sors
  module Decide
    PREFIX = 'command'

    def self.included(base)
      super
      base.extend ClassMethods
    end

    def decide(state, command)
      events = send(Sors.message_method_name(PREFIX, command.class.name), state, command)
      [events].flatten.compact
    end

    module ClassMethods
      def inherited(subclass)
        super
        handled_commands.each do |cmd_type|
          subclass.handled_commands << cmd_type
        end
      end

      def handle_command(_command)
        raise NoMethodError, "implement .handle_command(Command) in #{self}"
      end

      def handled_commands
        @handled_commands ||= []
      end

      def decide(cmd_type, &block)
        handled_commands << cmd_type
        define_method(Sors.message_method_name(PREFIX, cmd_type.name), &block)
      end
    end
  end
end
