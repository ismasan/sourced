# frozen_string_literal: true

# Shared message and reactor definitions for Sourced::Unit specs.
# Required by both spec/unit_spec.rb and spec/unit_sequel_postgres_spec.rb.
module UnitTest
  # Messages
  CreateThing = Sourced::Command.define('unittest.create_thing') do
    attribute :name, String
  end

  ThingCreated = Sourced::Event.define('unittest.thing_created') do
    attribute :name, String
  end

  NotifyThing = Sourced::Command.define('unittest.notify_thing') do
    attribute :name, String
  end

  ThingNotified = Sourced::Event.define('unittest.thing_notified') do
    attribute :name, String
  end

  # An Actor that handles CreateThing command, produces ThingCreated event,
  # and has a reaction that dispatches NotifyThing command.
  class ThingActor < Sourced::Actor
    state do |id|
      { id: id, name: nil, status: 'new' }
    end

    command CreateThing do |state, cmd|
      event ThingCreated, name: cmd.payload.name
    end

    event ThingCreated do |state, event|
      state[:name] = event.payload.name
      state[:status] = 'created'
    end

    reaction ThingCreated do |state, event|
      dispatch(NotifyThing, name: state[:name])
    end
  end

  # A second Actor that handles NotifyThing command
  class NotifierActor < Sourced::Actor
    state do |id|
      { id: id, notified: false, name: nil }
    end

    command NotifyThing do |state, cmd|
      event ThingNotified, name: cmd.payload.name
    end

    event ThingNotified do |state, event|
      state[:notified] = true
      state[:name] = event.payload.name
    end
  end

  # A projector that tracks ThingCreated events
  class ThingProjector < Sourced::Projector::StateStored
    state do |id|
      { id: id, things: [] }
    end

    event ThingCreated do |state, event|
      state[:things] << event.payload.name
    end
  end

  # A projector that tracks ThingNotified events
  class NotifiedProjector < Sourced::Projector::StateStored
    state do |id|
      { id: id, notifications: [] }
    end

    event ThingNotified do |state, event|
      state[:notifications] << event.payload.name
    end
  end

  # A projector with a catch-all reaction (reacts to all evolved events)
  class ReactingProjector < Sourced::Projector::StateStored
    state do |id|
      { id: id, things: [] }
    end

    event ThingCreated do |state, event|
      state[:things] << event.payload.name
    end

    reaction do |state, event|
      dispatch(NotifyThing, name: state[:things].last)
    end
  end

  # A projector that evolves multiple events, with a specific reaction on one
  # and a catch-all reaction covering the rest
  class MixedReactingProjector < Sourced::Projector::StateStored
    state do |id|
      { id: id, things: [], notifications: [] }
    end

    event ThingCreated do |state, event|
      state[:things] << event.payload.name
    end

    event ThingNotified do |state, event|
      state[:notifications] << event.payload.name
    end

    # Specific reaction for ThingCreated
    reaction ThingCreated do |state, event|
      dispatch(NotifyThing, name: state[:things].last)
    end

    # Catch-all covers ThingNotified (ThingCreated already has a specific reaction)
    reaction do |state, event|
      dispatch(CreateThing, name: 'from-catchall')
    end
  end

  # For infinite loop tests: an actor that reacts to its own events
  LoopCmd = Sourced::Command.define('unittest.loop_cmd')
  LoopEvent = Sourced::Event.define('unittest.loop_event')

  class LoopingActor < Sourced::Actor
    state do |id|
      { id: id }
    end

    command LoopCmd do |state, cmd|
      event LoopEvent
    end

    event LoopEvent do |state, event|
    end

    reaction LoopEvent do |state, event|
      dispatch(LoopCmd)
    end
  end

  # For scheduled messages tests
  ScheduleCmd = Sourced::Command.define('unittest.schedule_cmd')
  ScheduleEvent = Sourced::Event.define('unittest.schedule_event')
  DelayedCmd = Sourced::Command.define('unittest.delayed_cmd')

  class SchedulingActor < Sourced::Actor
    state do |id|
      { id: id }
    end

    command ScheduleCmd do |state, cmd|
      event ScheduleEvent
    end

    event ScheduleEvent do |state, event|
    end

    reaction ScheduleEvent do |state, event|
      dispatch(DelayedCmd).at(Time.now + 3600)
    end
  end

  # For testing bracket-accessor dispatch syntax: Reactor[:command_name]
  # Uses inline command definitions so that Reactor[] can resolve them.
  class InlineNotifierActor < Sourced::Actor
    state do |id|
      { id: id, notified: false }
    end

    command :notify_inline, name: String do |state, cmd|
      event :inline_notified, name: cmd.payload.name
    end

    event :inline_notified, name: String do |state, event|
      state[:notified] = true
    end
  end

  class BracketDispatchActor < Sourced::Actor
    state do |id|
      { id: id }
    end

    command CreateThing do |state, cmd|
      event ThingCreated, name: cmd.payload.name
    end

    event ThingCreated do |state, event|
      state[:name] = event.payload.name
    end

    reaction ThingCreated do |state, event|
      dispatch(InlineNotifierActor[:notify_inline], name: state[:name])
    end
  end

  # For testing sync actions
  SyncLog = []

  class SyncActor < Sourced::Actor
    state do |id|
      { id: id }
    end

    command CreateThing do |state, cmd|
      event ThingCreated, name: cmd.payload.name
    end

    event ThingCreated do |state, event|
      state[:name] = event.payload.name
    end

    sync do |command:, events:, state:|
      SyncLog << { command: command.class, events: events.map(&:class), state: state }
    end
  end
end
