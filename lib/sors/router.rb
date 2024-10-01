# frozen_string_literal: true

require 'singleton'

module Sors
  class Router
    include Singleton

    class << self
      def register(...)
        instance.register(...)
      end

      def register_machine(...)
        instance.register_machine(...)
      end

      def register_reactor(...)
        instance.register_reactor(...)
      end

      def handle(command)
        instance.handle(command)
      end

      def reactors_for(...)
        instance.reactors_for(...)
      end
    end

    def initialize
      @machines = {}
      @reactors = {}
    end

    def register(thing)
      if thing.respond_to?(:handled_commands)
        thing.handled_commands.each do |cmd_type|
          @machines[cmd_type] = thing
        end
      end
      return unless thing.respond_to?(:handled_reactions)

      thing.handled_reactions.each do |event_type|
        @reactors[event_type] ||= []
        @reactors[event_type] << thing
      end
    end

    def handle(command)
      machine = @machines.fetch(command.class)
      machine.handle(command)
    end

    def reactors_for(events)
      # test Array<Reactor>.uniq works
      events.each.with_object([]) do |event, list|
        reactors = @reactors[event.class] || []
        list.concat(reactors)
      end.flatten.uniq
    end
  end
end
