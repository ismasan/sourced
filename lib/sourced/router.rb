# frozen_string_literal: true

require 'singleton'

module Sourced
  class Router
    include Singleton

    class << self
      public :new

      def register(...)
        instance.register(...)
      end

      def handle(command)
        instance.handle(command)
      end

      def reactors
        instance.reactors
      end
    end

    attr_reader :reactors

    def initialize
      @decider_lookup = {}
      @reactor_lookup = {}
      @reactors = Set.new
    end

    def register(thing)
      if DeciderInterface === thing
        thing.handled_commands.each do |cmd_type|
          @decider_lookup[cmd_type] = thing
        end
      end

      return unless ReactorInterface === thing

      # TODO: we're not using this
      # thing.handled_events.each do |event_type|
      #   @reactor_lookup[event_type] ||= []
      #   @reactor_lookup[event_type] << thing
      # end

      @reactors << thing
    end

    def handle(command)
      decider = @decider_lookup.fetch(command.class)
      decider.handle_command(command)
    end
  end
end
