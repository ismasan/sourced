# frozen_string_literal: true

# Shared examples for Sourced::Unit.
# Include in a describe block that defines `let(:backend)`.
#
# @example With TestBackend
#   let(:backend) { Sourced::Backends::TestBackend.new }
#   it_behaves_like 'a unit'
#
# @example With SequelBackend
#   let(:backend) { Sourced::Backends::SequelBackend.new(db) }
#   it_behaves_like 'a unit'
RSpec.shared_examples 'a unit' do
  let(:stream_id) { "thing-#{SecureRandom.uuid}" }

  before do
    UnitTest::SyncLog.clear
  end

  describe 'full chain execution' do
    it 'runs command -> event -> reaction -> command -> event synchronously' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend:
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      results = unit.handle(cmd)

      # ThingActor should have produced ThingCreated
      thing_events = results.events_for(UnitTest::ThingActor)
      expect(thing_events.size).to eq(1)
      expect(thing_events.first).to be_a(UnitTest::ThingCreated)
      expect(thing_events.first.payload.name).to eq('Widget')

      # NotifierActor should have produced ThingNotified
      notifier_events = results.events_for(UnitTest::NotifierActor)
      expect(notifier_events.size).to eq(1)
      expect(notifier_events.first).to be_a(UnitTest::ThingNotified)

      # All messages should be in the backend
      stream_messages = backend.read_stream(stream_id)
      message_types = stream_messages.map(&:class)
      expect(message_types).to include(UnitTest::CreateThing)
      expect(message_types).to include(UnitTest::ThingCreated)
      expect(message_types).to include(UnitTest::NotifyThing)
      expect(message_types).to include(UnitTest::ThingNotified)
    end
  end

  describe 'multi-reactor handling' do
    it 'routes events to multiple reactors' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::ThingProjector,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      results = unit.handle(cmd)

      # ThingActor produced ThingCreated
      thing_events = results.events_for(UnitTest::ThingActor)
      expect(thing_events.size).to eq(1)

      # ThingProjector also received ThingCreated (evolves state)
      projector_results = results[UnitTest::ThingProjector]
      expect(projector_results).not_to be_empty
      instance = projector_results.keys.first
      expect(instance.state[:things]).to eq(['Widget'])
    end
  end

  describe 'offset tracking' do
    it 'ACKs messages so Router#drain finds no pending messages' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      unit.handle(cmd)

      # Set up a router with the same backend and reactor
      router = Sourced::Router.new(backend: backend)
      router.register(UnitTest::ThingActor)

      # drain should find nothing to process
      logs = []
      allow(UnitTest::ThingActor).to receive(:handle).and_wrap_original do |m, *args, **kwargs|
        logs << args.first.class
        m.call(*args, **kwargs)
      end

      router.drain
      expect(logs).to be_empty
    end
  end

  describe 'correlation chain' do
    it 'maintains correlation_id across the full chain' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      unit.handle(cmd)

      stream_messages = backend.read_stream(stream_id)
      # All messages after the initial command should share the same correlation_id
      correlation_ids = stream_messages.map(&:correlation_id).uniq
      expect(correlation_ids.size).to eq(1)
    end

    it 'propagates metadata from the initial command through the chain' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(
        stream_id: stream_id,
        payload: { name: 'Widget' },
        metadata: { request_id: 'req-abc', user_id: 'u-1' }
      )
      unit.handle(cmd)

      stream_messages = backend.read_stream(stream_id)
      # Every message after the initial command should carry the original metadata
      stream_messages.each do |msg|
        expect(msg.metadata[:request_id]).to eq('req-abc'), "#{msg.class} missing request_id"
        expect(msg.metadata[:user_id]).to eq('u-1'), "#{msg.class} missing user_id"
      end
    end
  end

  describe 'infinite loop prevention' do
    it 'raises InfiniteLoopError when max_iterations exceeded' do
      unit = Sourced::Unit.new(
        UnitTest::LoopingActor,
        backend: backend,
        max_iterations: 5
      )

      cmd = UnitTest::LoopCmd.new(stream_id: stream_id)
      expect {
        unit.handle(cmd)
      }.to raise_error(Sourced::Unit::InfiniteLoopError, /Exceeded 5 iterations/)
    end
  end

  describe 'transaction rollback' do
    it 'rolls back all changes on error' do
      error_actor = Class.new(Sourced::Actor) do
        extend Sourced::Consumer

        consumer do |c|
          c.group_id = 'UnitTest::ErrorActor'
        end

        state do |id|
          { id: id }
        end

        command UnitTest::ThingCreated do |state, cmd|
          raise 'Boom!'
        end

        event UnitTest::ThingCreated do |state, event|
        end
      end

      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        error_actor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      expect {
        unit.handle(cmd)
      }.to raise_error(RuntimeError, 'Boom!')

      # Backend should have no messages due to rollback
      expect(backend.read_stream(stream_id)).to be_empty
    end
  end

  describe 'scheduled messages' do
    it 'schedules messages but does not execute them synchronously' do
      unit = Sourced::Unit.new(
        UnitTest::SchedulingActor,
        backend: backend
      )

      cmd = UnitTest::ScheduleCmd.new(stream_id: stream_id)
      results = unit.handle(cmd)

      # The ScheduleEvent should be produced
      events = results.events_for(UnitTest::SchedulingActor)
      expect(events.size).to eq(1)
      expect(events.first).to be_a(UnitTest::ScheduleEvent)

      # DelayedCmd should NOT appear in the stream (it's scheduled for later)
      stream_messages = backend.read_stream(stream_id)
      expect(stream_messages.map(&:class)).not_to include(UnitTest::DelayedCmd)
    end
  end

  describe 'unhandled messages' do
    it 'appends messages not handled by unit reactors for background workers' do
      # Unit only has ThingActor, not NotifierActor
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      unit.handle(cmd)

      # NotifyThing command should still be in the store (from reaction's AppendNext)
      stream_messages = backend.read_stream(stream_id)
      message_types = stream_messages.map(&:class)
      expect(message_types).to include(UnitTest::NotifyThing)
    end
  end

  describe 'Results API' do
    it 'returns instance and produced events per reactor class' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::ThingProjector,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      results = unit.handle(cmd)

      # results[ThingActor] returns { instance => [events] }
      actor_results = results[UnitTest::ThingActor]
      expect(actor_results.size).to eq(1)

      instance, events = actor_results.first
      expect(instance).to be_a(UnitTest::ThingActor)
      expect(instance.state[:name]).to eq('Widget')
      expect(instance.state[:status]).to eq('created')
      expect(events.size).to eq(1)
      expect(events.first).to be_a(UnitTest::ThingCreated)

      # results[ThingProjector] returns { instance => [events] }
      projector_results = results[UnitTest::ThingProjector]
      expect(projector_results.size).to eq(1)

      proj_instance, proj_events = projector_results.first
      expect(proj_instance).to be_a(UnitTest::ThingProjector)
      # Projectors don't produce events (AppendAfter), so this is empty
      expect(proj_events).to be_empty
    end
  end

  describe 'sync actions' do
    it 'executes sync actions within the transaction' do
      unit = Sourced::Unit.new(
        UnitTest::SyncActor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      unit.handle(cmd)

      expect(UnitTest::SyncLog.size).to eq(1)
      log = UnitTest::SyncLog.first
      expect(log[:command]).to eq(UnitTest::CreateThing)
      expect(log[:events].first).to eq(UnitTest::ThingCreated)
    end
  end

  describe 'persist_commands: false' do
    it 'does not persist commands but events are always persisted' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend: backend,
        persist_commands: false
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      results = unit.handle(cmd)

      # Full chain still runs synchronously
      thing_events = results.events_for(UnitTest::ThingActor)
      expect(thing_events.size).to eq(1)
      expect(thing_events.first).to be_a(UnitTest::ThingCreated)

      notifier_events = results.events_for(UnitTest::NotifierActor)
      expect(notifier_events.size).to eq(1)
      expect(notifier_events.first).to be_a(UnitTest::ThingNotified)

      # Events should be in the store
      stream_messages = backend.read_stream(stream_id)
      message_types = stream_messages.map(&:class)
      expect(message_types).to include(UnitTest::ThingCreated)
      expect(message_types).to include(UnitTest::ThingNotified)

      # Commands should NOT be in the store
      expect(message_types).not_to include(UnitTest::CreateThing)
      expect(message_types).not_to include(UnitTest::NotifyThing)
    end

    it 'still runs the full BFS chain even though commands are not persisted' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend: backend,
        persist_commands: false
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      results = unit.handle(cmd)

      # Both actors' state should be updated
      actor_results = results[UnitTest::ThingActor]
      expect(actor_results.values.flatten.size).to eq(1)

      notifier_results = results[UnitTest::NotifierActor]
      expect(notifier_results.values.flatten.size).to eq(1)
    end
  end

  describe 'persist_commands: true (default)' do
    it 'persists all messages including commands' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend: backend
      )

      cmd = UnitTest::CreateThing.new(stream_id: stream_id, payload: { name: 'Widget' })
      unit.handle(cmd)

      stream_messages = backend.read_stream(stream_id)
      message_types = stream_messages.map(&:class)
      expect(message_types).to include(UnitTest::CreateThing)
      expect(message_types).to include(UnitTest::ThingCreated)
      expect(message_types).to include(UnitTest::NotifyThing)
      expect(message_types).to include(UnitTest::ThingNotified)
    end
  end

  describe 'reusability' do
    it 'can handle multiple commands without instance state mutation' do
      unit = Sourced::Unit.new(
        UnitTest::ThingActor,
        UnitTest::NotifierActor,
        backend: backend
      )

      stream_id_1 = "#{stream_id}-1"
      stream_id_2 = "#{stream_id}-2"

      results1 = unit.handle(UnitTest::CreateThing.new(stream_id: stream_id_1, payload: { name: 'First' }))
      results2 = unit.handle(UnitTest::CreateThing.new(stream_id: stream_id_2, payload: { name: 'Second' }))

      expect(results1.events_for(UnitTest::ThingActor).first.payload.name).to eq('First')
      expect(results2.events_for(UnitTest::ThingActor).first.payload.name).to eq('Second')

      expect(backend.read_stream(stream_id_1).size).to eq(4)
      expect(backend.read_stream(stream_id_2).size).to eq(4)
    end
  end
end
