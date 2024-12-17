# frozen_string_literal: true

require 'spec_helper'

module TestDecider
  TodoList = Struct.new(:archive_status, :seq, :id, :status, :items)

  AddItem = Sourced::Message.define('decider.todos.add') do
    attribute :name, String
  end

  Notify = Sourced::Message.define('decider.todos.notify')

  ListStarted = Sourced::Message.define('decider.todos.started')
  ArchiveRequested = Sourced::Message.define('decider.todos.archive_requested')
  ConfirmArchive = Sourced::Message.define('decider.todos.archive_confirm')
  ArchiveConfirmed = Sourced::Message.define('decider.todos.archive_confirmed')

  class TodoListDecider < Sourced::Decider
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

    react ArchiveRequested do |_event|
      command ConfirmArchive
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

    react :item_added do |_event|
      command Notify
    end

    command Notify do |list, cmd|
    end
  end

  class ChildDecider < TodoListDecider
    command :add_more, name: String do |_list, cmd|
      event :item_added, name: cmd.payload.name
    end
  end

  Sourced::Router.register(TodoListDecider)

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

  class WithSync < Sourced::Decider
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
end

RSpec.describe Sourced::Decider do
  before do
    Sourced.config.backend.clear!
  end

  let(:cmd) { TestDecider::AddItem.parse(stream_id: 'list1', payload: { name: 'item1' }) }

  describe '#decide' do
    let(:decider) {  TestDecider::TodoListDecider.new(cmd.stream_id) }

    it 'takes command, evolves state' do
      list, _events = decider.decide(cmd)
      expect(list.status).to eq(:open)
      expect(list.items.map(&:name)).to eq(%w[item1])
    end

    it 'produces events with incrementing sequences' do
      _list, events = decider.decide(cmd)
      expect(events.map(&:seq)).to eq([1, 2, 3])
      expect(events.map(&:type)).to eq(%w[decider.todos.add decider.todos.started
                                          test_decider.todo_list_decider.item_added])
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
                                                              test_decider.todo_list_decider.item_added])
    end
  end

  describe '.handle_command' do
    it 'appends events to store' do
      TestDecider::TodoListDecider.handle_command(cmd)
      events = Sourced.config.backend.read_event_stream(cmd.stream_id)
      expect(events.map(&:seq)).to eq([1, 2, 3])
      expect(events.map(&:type)).to eq(%w[decider.todos.add decider.todos.started
                                          test_decider.todo_list_decider.item_added])
      expect(events.map { |e| e.metadata[:producer] }).to eq([
                                                               nil,
                                                               TestDecider::TodoListDecider.consumer_info.group_id,
                                                               TestDecider::TodoListDecider.consumer_info.group_id
                                                             ])
    end
  end

  describe '.load' do
    it 'loads state from event history' do
      TestDecider::TodoListDecider.handle_command(cmd)
      decider = TestDecider::TodoListDecider.load(cmd.stream_id)
      expect(decider.seq).to eq(3)
      expect(decider.state.items.map(&:name)).to eq(%w[item1])
    end
  end

  specify '.handled_commands' do
    expect(TestDecider::TodoListDecider.handled_commands).to eq([
                                                                  TestDecider::AddItem,
                                                                  TestDecider::TodoListDecider::AddOne,
                                                                  TestDecider::TodoListDecider::Archive,
                                                                  TestDecider::ConfirmArchive,
                                                                  TestDecider::Notify
                                                                ])
  end

  specify '.handled_events' do
    expect(TestDecider::TodoListDecider.handled_events).to eq([
                                                                TestDecider::ArchiveRequested,
                                                                TestDecider::TodoListDecider::ItemAdded
                                                              ])
  end

  describe 'definining inline commands' do
    specify '.[](message_name)' do
      expect(TestDecider::TodoListDecider[:item_added]).to eq(TestDecider::TodoListDecider::ItemAdded)
    end

    it 'uses Ruby namespace as message type prefix by default' do
      event_class = TestDecider::TodoListDecider[:item_added]
      expect(event_class.type).to eq('test_decider.todo_list_decider.item_added')
    end

    it 'can define a custom prefix via .message_namespace' do
      klass = Class.new(Sourced::Decider) do
        def self.message_namespace = 'my_namespace'
        command :do_something do |_list, _cmd|
        end
      end
      expect(klass[:do_something].type).to eq('my_namespace.do_something')
    end
  end

  specify '.handle_events' do
    evt = TestDecider::TodoListDecider::ItemAdded.parse(stream_id: 'list1', payload: { name: 'item1' })
    commands = TestDecider::TodoListDecider.handle_events([evt])
    expect(commands.map(&:class)).to eq([TestDecider::Notify])
    expect(commands.first.stream_id).to eq('list1')
    expect(commands.first.causation_id).to eq(evt.id)
  end

  specify '#catch_up' do
    TestDecider::TodoListDecider.handle_command(cmd)
    decider = TestDecider::TodoListDecider.load(cmd.stream_id)
    expect(decider.seq).to eq(3)
    cmd2 = TestDecider::AddItem.parse(stream_id: cmd.stream_id, payload: { name: 'item2' })
    TestDecider::TodoListDecider.handle_command(cmd2)
    expect(decider.catch_up).to eq([3, 5])
    expect(decider.seq).to eq(5)
  end

  specify 'command DSL' do
    decider = TestDecider::TodoListDecider.new('list1')
    cmd = decider.add_one(name: 'item1')
    expect(cmd.valid?).to eq(true)
    expect(decider.seq).to eq(2)
  end

  it 'returns if invalid command' do
    decider = TestDecider::TodoListDecider.new('list1')
    cmd = decider.add_one(name: 10)
    expect(cmd.valid?).to be(false)
    expect(decider.state.items.size).to eq(0)
  end

  specify '[command]_async' do
    decider = TestDecider::TodoListDecider.new('list1')
    cmd = decider.add_one_async(name: 'item1')
    expect(cmd).to be_a(TestDecider::TodoListDecider::AddOne)
    expect(cmd.valid?).to eq(true)
    expect(Sourced.config.backend.next_command).to eq(cmd)
  end

  specify '[command]_later' do
    decider = TestDecider::TodoListDecider.new('list1')
    later = Time.now + 60
    cmd = decider.add_one_later(later, name: 'item1')
    expect(cmd).to be_a(TestDecider::TodoListDecider::AddOne)
    expect(cmd.valid?).to eq(true)
    expect(Sourced.config.backend.next_command).to eq(cmd)
    expect(cmd.created_at).to eq(later)
  end

  specify '#events' do
    decider = TestDecider::TodoListDecider.new('list1')
    decider.add_one(name: 'item1')
    events = decider.events
    expect(events.map(&:seq)).to eq([1, 2])
    expect(events.map(&:class)).to eq([
                                        TestDecider::TodoListDecider::AddOne,
                                        TestDecider::TodoListDecider::ItemAdded
                                      ])

    TestDecider::TodoListDecider.handle_command(cmd)
    expect(decider.events.map(&:seq)).to eq([1, 2])
    expect(decider.events(upto: nil).map(&:seq)).to eq([1, 2, 3, 4, 5])
  end

  specify 'reacting to events' do
    decider = TestDecider::TodoListDecider.new('list1')
    decider.add_one(name: 'Buy milk')
    decider.add_one(name: 'Buy bread')

    decider.archive
    expect(decider.state.archive_status).to eq(:requested)

    Sourced::Worker.drain

    decider.catch_up
    expect(decider.state.archive_status).to eq(:confirmed)
  end

  describe '.sync' do
    before do
      allow(TestDecider::Listener).to receive(:call)
    end

    specify 'with a .call(state, command, events) interface' do
      decider = TestDecider::WithSync.new('id')
      decider.do_thing
      expect(TestDecider::Listener).to have_received(:call) do |state, command, events|
        expect(state).to eq(decider.state)
        expect(command).to be_a(TestDecider::WithSync::DoThing)
        expect(events.map(&:class)).to eq([TestDecider::WithSync::ThingDone])
      end
    end

    specify 'raising an exception cancels append transaction' do
      allow(TestDecider::Listener).to receive(:call).and_raise('boom')
      decider = TestDecider::WithSync.new('id')
      expect { decider.do_thing }.to raise_error('boom')
      expect(Sourced.config.backend.read_event_stream('id')).to be_empty
    end

    specify 'with a Reactor interface it calls #handle_events and ACKs group offsets' do
      allow(TestDecider::DummyProjector).to receive(:handle_events)

      decider = TestDecider::WithSync.new('id')
      decider.do_thing
      expect(TestDecider::DummyProjector).to have_received(:handle_events) do |events|
        expect(events.map(&:class)).to eq([TestDecider::WithSync::ThingDone])
      end

      group = Sourced.config.backend.stats.groups.first
      expect(group[:group_id]).to eq('TestDecider::DummyProjector')
      expect(group[:stream_count]).to eq(1)
      expect(group[:oldest_processed]).to eq(2)
    end
  end

  describe 'inheritance' do
    it 'copies over .handled_events' do
      expect(TestDecider::ChildDecider.handled_events).to eq(TestDecider::TodoListDecider.handled_events)
    end

    it 'copies over .handled_commands and appends its own commands' do
      expect(TestDecider::ChildDecider.handled_commands).to eq([
                                                                 *TestDecider::TodoListDecider.handled_commands,
                                                                 TestDecider::ChildDecider::AddMore
                                                               ])
    end

    it 'has its own event registry' do
      expect(TestDecider::ChildDecider[:add_more]).to eq(TestDecider::ChildDecider::AddMore)
    end
  end
end
