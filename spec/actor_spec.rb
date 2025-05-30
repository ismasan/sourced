# frozen_string_literal: true

require 'spec_helper'

module TestActor
  TodoList = Struct.new(:archive_status, :seq, :id, :status, :items)

  AddItem = Sourced::Message.define('decider.todos.add') do
    attribute :name, String
  end

  Notify = Sourced::Message.define('decider.todos.notify')

  ListStarted = Sourced::Message.define('decider.todos.started')
  ArchiveRequested = Sourced::Message.define('decider.todos.archive_requested')
  ConfirmArchive = Sourced::Message.define('decider.todos.archive_confirm')
  ArchiveConfirmed = Sourced::Message.define('decider.todos.archive_confirmed')

  class TodoListActor < Sourced::Actor
    consumer do |c|
      c.async!
    end

    state do |id|
      TodoList.new(nil, 0, id, :new, [])
    end

    command AddItem do |list, cmd|
      event ListStarted if list.status == :new
      event :item_added, name: cmd.payload.name
    end

    # Command DSL
    command :add_one, name: String do |_list, cmd|
      event :item_added, name: cmd.payload.name
    end

    command :archive do |_list, _cmd|
      event ArchiveRequested
    end

    reaction ArchiveRequested do |event|
      stream_for(event).command ConfirmArchive
    end

    command ConfirmArchive do |_list, _cmd|
      event ArchiveConfirmed
    end

    event ArchiveRequested do |list, _event|
      list.archive_status = :requested
    end

    event ArchiveConfirmed do |list, _event|
      list.archive_status = :confirmed
    end

    before_evolve do |list, event|
      list.seq = event.seq
    end

    event ListStarted do |list, _event|
      list.status = :open
    end

    event :item_added, name: String do |list, event|
      list.items << event.payload
    end

    reaction :item_added do |event|
      stream_for(event).command Notify
    end

    command Notify do |list, cmd|
    end
  end

  class ChildActor < TodoListActor
    command :add_more, name: String do |_list, cmd|
      event :item_added, name: cmd.payload.name
    end
  end

  Sourced.register(TodoListActor)

  class Listener
    def self.call(state, command, events); end
  end

  class DummyProjector
    extend Sourced::Consumer

    consumer do |c|
      c.async!
    end

    class << self
      def handled_events = [WithSyncReactor::ThingDone]

      def handle_events(_events)
        []
      end
    end
  end

  class WithSync < Sourced::Actor
    ThingDone = Sourced::Message.define('with_sync_callable.thing_done')

    def init_state(id)
      id
    end

    command :do_thing do |_, _cmd|
      event(ThingDone)
    end

    sync Listener
    sync DummyProjector
  end

  class ReactorSymbolTest < Sourced::Actor
    consumer do |c|
      c.sync!
    end

    state do |id|
      [id, :new, nil]
    end

    command :do_thing, name: String do |_state, cmd|
      event :thing_done, cmd.payload
    end

    event :thing_done, name: String do |state, _event|
    end

    reaction :thing_done do |event|
      stream_for(event).command(:notify, value: 'done!').delay(Time.now + 1)
    end

    command :notify, value: String do |_state, cmd|
      event :notified, cmd.payload
    end

    event :notified, value: String do |state, event|
      state[1] = :notified
      state[2] = event.payload.value
    end
  end

  class ReactorSecondStreamTest < Sourced::Actor
    consumer do |c|
      c.sync!
    end

    state do |id|
      [id, :new, nil]
    end

    command :do_thing, name: String do |_state, cmd|
      event :thing_done, cmd.payload
    end

    event :thing_done, name: String do |state, _event|
    end

    reaction :thing_done do |event|
      stream_for(ReactorSymbolTest).command(:notify, value: 'done from ReactorSecondStreamTest!')
    end
  end
end

RSpec.describe Sourced::Actor do
  before do
    Sourced.config.backend.clear!
  end

  let(:cmd) { TestActor::AddItem.parse(stream_id: 'list1', payload: { name: 'item1' }) }

  describe '#decide' do
    let(:decider) {  TestActor::TodoListActor.new(cmd.stream_id) }

    it 'takes command, evolves state' do
      list, _events = decider.decide(cmd)
      expect(list.status).to eq(:open)
      expect(list.items.map(&:name)).to eq(%w[item1])
    end

    it 'produces events with incrementing sequences' do
      _list, events = decider.decide(cmd)
      expect(events.map(&:seq)).to eq([1, 2, 3])
      expect(events.map(&:type)).to eq(%w[decider.todos.add decider.todos.started
                                          test_actor.todo_list_actor.item_added])
    end

    it 'increments #seq' do
      list, = decider.decide(cmd)
      expect(decider.seq).to eq(3)
      expect(list.seq).to eq(3)
    end

    it 'tracks #uncommitted_events' do
      decider.decide(cmd)
      expect(decider.uncommitted_events.map(&:seq)).to eq([1, 2, 3])
      expect(decider.uncommitted_events.map(&:type)).to eq(%w[decider.todos.add decider.todos.started
                                                              test_actor.todo_list_actor.item_added])
    end
  end

  describe '.handle_command' do
    it 'appends events to store' do
      TestActor::TodoListActor.handle_command(cmd)
      events = Sourced.config.backend.read_event_stream(cmd.stream_id)
      expect(events.map(&:seq)).to eq([1, 2, 3])
      expect(events.map(&:type)).to eq(%w[decider.todos.add decider.todos.started
                                          test_actor.todo_list_actor.item_added])
      expect(events.map { |e| e.metadata[:producer] }).to eq([
                                                               nil,
                                                               TestActor::TodoListActor.consumer_info.group_id,
                                                               TestActor::TodoListActor.consumer_info.group_id
                                                             ])
    end
  end

  describe '.load' do
    it 'loads state from event history' do
      TestActor::TodoListActor.handle_command(cmd)
      decider = TestActor::TodoListActor.load(cmd.stream_id)
      expect(decider.seq).to eq(3)
      expect(decider.state.items.map(&:name)).to eq(%w[item1])
    end
  end

  specify '.handled_commands' do
    expect(TestActor::TodoListActor.handled_commands).to eq([
                                                              TestActor::AddItem,
                                                              TestActor::TodoListActor::AddOne,
                                                              TestActor::TodoListActor::Archive,
                                                              TestActor::ConfirmArchive,
                                                              TestActor::Notify
                                                            ])
  end

  specify '.handled_events' do
    expect(TestActor::TodoListActor.handled_events).to eq([
                                                            TestActor::ArchiveRequested,
                                                            TestActor::TodoListActor::ItemAdded
                                                          ])
  end

  describe 'definining inline commands' do
    specify '.[](message_name)' do
      expect(TestActor::TodoListActor[:item_added]).to eq(TestActor::TodoListActor::ItemAdded)
    end

    it 'uses Ruby namespace as message type prefix by default' do
      event_class = TestActor::TodoListActor[:item_added]
      expect(event_class.type).to eq('test_actor.todo_list_actor.item_added')
    end

    it 'can define a custom prefix via .message_namespace' do
      klass = Class.new(Sourced::Actor) do
        def self.message_namespace = 'my_namespace'
        command :do_something do |_list, _cmd|
        end
      end
      expect(klass[:do_something].type).to eq('my_namespace.do_something')
    end
  end

  specify '.handle_events' do
    evt = TestActor::TodoListActor::ItemAdded.parse(stream_id: 'list1', payload: { name: 'item1' })
    commands = TestActor::TodoListActor.handle_events([evt], replaying: false)
    expect(commands.map(&:class)).to eq([TestActor::Notify])
    expect(commands.first.stream_id).to eq('list1')
    expect(commands.first.causation_id).to eq(evt.id)
  end

  specify '#catch_up' do
    TestActor::TodoListActor.handle_command(cmd)
    decider = TestActor::TodoListActor.load(cmd.stream_id)
    expect(decider.seq).to eq(3)
    cmd2 = TestActor::AddItem.parse(stream_id: cmd.stream_id, payload: { name: 'item2' })
    TestActor::TodoListActor.handle_command(cmd2)
    expect(decider.catch_up).to eq([3, 5])
    expect(decider.seq).to eq(5)
  end

  specify 'command DSL' do
    decider = TestActor::TodoListActor.new('list1')
    cmd = decider.add_one(name: 'item1')
    expect(cmd.valid?).to eq(true)
    expect(decider.seq).to eq(2)
  end

  specify '#event with no set command (ex. applying events in tests)' do
    decider = TestActor::TodoListActor.new('list1')
    decider.event(TestActor::TodoListActor::ItemAdded, name: 'item1')
    expect(decider.seq).to eq(1)
    expect(decider.state.items.size).to eq(1)
  end

  it 'returns if invalid command' do
    decider = TestActor::TodoListActor.new('list1')
    cmd = decider.add_one(name: 10)
    expect(cmd.valid?).to be(false)
    expect(decider.state.items.size).to eq(0)
  end

  specify '[command]_async' do
    decider = TestActor::TodoListActor.new('list1')
    cmd = decider.add_one_async(name: 'item1')
    expect(cmd).to be_a(TestActor::TodoListActor::AddOne)
    expect(cmd.valid?).to eq(true)
    expect(Sourced.config.backend.next_command).to eq(cmd)
  end

  specify '[command]_later' do
    decider = TestActor::TodoListActor.new('list1')
    later = Time.now + 60
    cmd = decider.add_one_later(later, name: 'item1')
    expect(cmd).to be_a(TestActor::TodoListActor::AddOne)
    expect(cmd.valid?).to eq(true)
    expect(Sourced.config.backend.next_command).to eq(cmd)
    expect(cmd.created_at).to eq(later)
  end

  specify '#events' do
    decider = TestActor::TodoListActor.new('list1')
    decider.add_one(name: 'item1')
    events = decider.events
    expect(events.map(&:seq)).to eq([1, 2])
    expect(events.map(&:class)).to eq([
                                        TestActor::TodoListActor::AddOne,
                                        TestActor::TodoListActor::ItemAdded
                                      ])

    TestActor::TodoListActor.handle_command(cmd)
    expect(decider.events.map(&:seq)).to eq([1, 2])
    expect(decider.events(upto: nil).map(&:seq)).to eq([1, 2, 3, 4, 5])
  end

  specify 'reacting to events' do
    decider = TestActor::TodoListActor.new('list1')
    decider.add_one(name: 'Buy milk')
    decider.add_one(name: 'Buy bread')

    decider.archive
    expect(decider.state.archive_status).to eq(:requested)

    Sourced::Worker.drain

    decider.catch_up
    expect(decider.state.archive_status).to eq(:confirmed)
  end

  describe '.react producing own commands as Symbols' do
    before do
      # Register so that sync! works
      Sourced.register(TestActor::ReactorSymbolTest)
      Sourced.register(TestActor::ReactorSecondStreamTest)
    end

    it 'resolves own commands by Symbol' do
      actor = TestActor::ReactorSymbolTest.new('1')
      actor.do_thing(name: 'thing1')
      actor.catch_up
      expect(actor.state).to eq(['1', :notified, 'done!'])
      expect(actor.seq).to eq(4)
    end

    it "resolves another reactor's command by symbol" do
      actor = TestActor::ReactorSecondStreamTest.new('1')
      actor.do_thing(name: 'thing1')

      Sourced::Worker.drain

      events = Sourced.config.backend.events
      expect(events.map(&:class)).to eq([
        TestActor::ReactorSecondStreamTest::DoThing, 
        TestActor::ReactorSecondStreamTest::ThingDone, 
        TestActor::ReactorSymbolTest::Notify, 
        TestActor::ReactorSymbolTest::Notified
      ])

      stream_ids = events.map(&:stream_id)

      expect(stream_ids[0]).to eq('1')
      expect(stream_ids[1]).to eq('1')
      expect(stream_ids[2]).not_to eq('1')
      expect(stream_ids[3]).to eq(stream_ids[2])

      expect(events[0].causation_id).to eq(events[0].id)
      expect(events[1].causation_id).to eq(events[0].id)
      expect(events[2].causation_id).to eq(events[1].id)
      expect(events[3].causation_id).to eq(events[2].id)

      expect(events.map(&:correlation_id).uniq).to eq([events[0].id])
    end
  end

  describe '.reaction_with_state for own events' do
    let(:klass) do
      Class.new(Sourced::Actor) do
        consumer do |c|
          c.group_id = 'ReactTest'
          c.sync!
        end

        state do |id|
          [id, :new, nil]
        end

        command :do_thing, name: String do |_state, cmd|
          event :thing_done, cmd.payload
        end

        event :thing_done, name: String do |state, _event|
          state[1] = :done
        end

        reaction_with_state :thing_done do |state, event|
          stream_for(event).command :notify, value: "seq was #{seq}, state was #{state[1]}, name was #{event.payload.name}"
          return # <= should not raise LocalJumpError
        end

        command :notify, value: String do |_state, cmd|
          event :notified, cmd.payload
        end

        event :notified, value: String do |state, event|
          state[2] = event.payload.value
        end
      end
    end

    before do
      # Register so that sync! works
      Sourced.register(klass)
    end

    it 'evolves and yields own state' do
      decider = klass.new('1')
      decider.do_thing(name: 'thing1')
      decider.catch_up
      expect(decider.state).to eq(['1', :done, 'seq was 2, state was done, name was thing1'])
      expect(decider.seq).to eq(4)
    end

    it 'fails to register reaction if event is not handled by the same Actor' do
      expect do
        klass.reaction_with_state(TestActor::ListStarted) { |_state, _event| }
      end.to raise_error(ArgumentError)
    end

    it 'fails to register reaction if block does not support |state, event|' do
      expect do
        klass.reaction_with_state(TestActor::ListStarted) { |_state| }
      end.to raise_error(ArgumentError)
    end
  end

  describe '.reaction_with_state for all own events' do
    let(:klass) do
      Class.new(Sourced::Actor) do
        state do |id|
          [id, :new, nil]
        end

        command :do_thing, name: String do |_state, cmd|
          event :thing_done, cmd.payload
        end

        event :thing_done, name: String do |state, _event|
          state[1] = :done
        end

        command :notify, value: String do |_state, _cmd|
        end

        reaction_with_state do |state, event|
          stream_for(event).command :notify, value: "seq was #{seq}, state was #{state[1]}, name was #{event.payload.name}"
        end
      end
    end

    it 'evolves and yields own state, returning commands' do
      actor = klass.new('1')
      actor.do_thing(name: 'thing1')

      e1 = klass[:thing_done].parse(stream_id: actor.id, payload: { name: 'thing1' })
      commands = klass.handle_events([e1], replaying: false)
      expect(commands.map(&:class)).to eq([klass::Notify])
      expect(commands.first.payload.value).to eq('seq was 2, state was done, name was thing1')
    end
  end

  describe '.sync' do
    before do
      allow(TestActor::Listener).to receive(:call)
    end

    specify 'with a .call(state, command, events) interface' do
      decider = TestActor::WithSync.new('id')
      decider.do_thing
      expect(TestActor::Listener).to have_received(:call) do |state, command, events|
        expect(state).to eq(decider.state)
        expect(command).to be_a(TestActor::WithSync::DoThing)
        expect(events.map(&:class)).to eq([TestActor::WithSync::ThingDone])
      end
    end

    specify 'raising an exception cancels append transaction' do
      allow(TestActor::Listener).to receive(:call).and_raise('boom')
      decider = TestActor::WithSync.new('id')
      expect { decider.do_thing }.to raise_error('boom')
      expect(Sourced.config.backend.read_event_stream('id')).to be_empty
    end

    specify 'with a Reactor interface it calls #handle_events and ACKs group offsets' do
      allow(TestActor::DummyProjector).to receive(:handle_events)

      decider = TestActor::WithSync.new('id')
      decider.do_thing
      expect(TestActor::DummyProjector).to have_received(:handle_events) do |events|
        expect(events.map(&:class)).to eq([TestActor::WithSync::ThingDone])
      end

      group = Sourced.config.backend.stats.groups.first
      expect(group[:group_id]).to eq('TestActor::DummyProjector')
      expect(group[:stream_count]).to eq(1)
      expect(group[:oldest_processed]).to eq(2)
    end
  end

  describe 'inheritance' do
    it 'copies over .handled_events' do
      expect(TestActor::ChildActor.handled_events).to eq(TestActor::TodoListActor.handled_events)
    end

    it 'copies over .handled_commands and appends its own commands' do
      expect(TestActor::ChildActor.handled_commands).to eq([
                                                             *TestActor::TodoListActor.handled_commands,
                                                             TestActor::ChildActor::AddMore
                                                           ])
    end

    it 'has its own event registry' do
      expect(TestActor::ChildActor[:add_more]).to eq(TestActor::ChildActor::AddMore)
    end
  end
end
