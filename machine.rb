# frozen_string_literal: true

require_relative 'router'
require_relative 'reactor'

class Machine
  INLINE_SCHEDULER = proc do |commands|
    commands.each { |cmd| Router.handle(cmd) }
  end

  class << self
    def scheduler
      @scheduler ||= INLINE_SCHEDULER
    end

    def scheduler=(scheduler)
      @scheduler = scheduler
    end

    def handled_commands
      @handled_commands ||= []
    end

    def handled_events
      @handled_events ||= []
    end

    def persister
      @persister ||= ->(state, command, events) { Rails.logger.info("Persisting #{state}, #{command}, #{events}") }
    end

    def loader
      @loader ||= ->(command) { raise NotImplementedError.new("No loader defined for #{self}") }
    end

    def reactor
      @reactor ||= Class.new(Reactor)
    end

    def load(loader = nil, &block)
      @loader = loader || block
    end

    def decide(cmd_type, &block)
      handled_commands << cmd_type
      define_method(message_method_name(cmd_type.name), &block)
    end

    def evolve(event_type, &block)
      handled_events << event_type
      define_method(message_method_name(event_type.name), &block)
    end

    def persist(&block)
      @persister = block
    end

    def react(event_type, &block)
      reactor.react(event_type, &block)
    end

    def message_method_name(name)
      "__handle_#{name.split('::').map(&:downcase).join('_')}"
    end
  end

  def initialize
  end

  def handle(command)
    puts "Handling #{command.type}"
    state = load_state(command)
    events = handle_command(state, command)
    state = handle_events(state, events)
    commands = handle_reactions(state, events)
    transaction do
      persist(state, command, events)
      schedule_commands(commands)
    end
    [ state, events ]
  end

  private

  def load_state(command)
    self.class.loader.call(command)
  end

  def handle_command(state, command)
    events = send(self.class.message_method_name(command.class.name), state, command)
    [ events ].flatten.compact
  end

  def handle_events(state, events)
    events.each do |event|
      method_name = self.class.message_method_name(event.class.name)
      send(method_name, state, event) if respond_to?(method_name)
    end

    state
  end

  def handle_reactions(_state, events)
    self.class.reactor.call(events)
  end

  def persist(state, command, events)
    self.class.persister.call(state, command, events)
  end

  def schedule_commands(commands)
    self.class.scheduler.call(commands)
  end

  def transaction(&)
    yield
  end
end
