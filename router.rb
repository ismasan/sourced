# frozen_string_literal: true

require 'singleton'

class Router
  include Singleton

  class << self
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

  def register_machine(machine)
    machine.handled_commands.each do |cmd_type|
      @machines[cmd_type] = machine
    end

    register_reactor(machine.reactor)
  end

  def register_reactor(reactor)
    reactor.handled_events.each do |event_type|
      @reactors[event_type] ||= []
      @reactors[event_type] << reactor
    end
  end

  def handle(command)
    machine = @machines.fetch(command.class)
    machine.new.handle(command)
  end

  def reactors_for(events)
    # test Array<Reactor>.uniq works
    events.each.with_object([]) do |event, list|
      reactors = @reactors[event.class] || []
      list.concat(reactors)
    end.flatten.uniq
  end

  def react_to(events)
    events.each do |event|
      reactors = @reactors[event.class]
      next unless reactors

      # Should schedule each reactor in its own thread
      # or worker
      reactors.each do |reactor|
        cmds = reactor.handle(event)
      end
    end
  end
end
