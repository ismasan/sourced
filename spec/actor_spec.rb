# frozen_string_literal: true

require 'spec_helper'

module TestDomain
  TodoList = Struct.new(:archive_status, :seq, :id, :status, :items)

  AddItem = Sourced::Message.define('actor.todos.add') do
    attribute :name, String
  end

  Notify = Sourced::Message.define('actor.todos.notify') do
    attribute :item_count, Integer
  end

  ListStarted = Sourced::Message.define('actor.todos.started')
  ArchiveRequested = Sourced::Message.define('actor.todos.archive_requested')
  ConfirmArchive = Sourced::Message.define('actor.todos.archive_confirm')
  ArchiveConfirmed = Sourced::Message.define('actor.todos.archive_confirmed')

  class Tracer
    attr_reader :calls

    def initialize
      @calls = []
    end
  end

  SyncTracer = Tracer.new

  class TodoListActor < Sourced::Actor
    state do |id|
      TodoList.new(nil, 0, id, :new, [])
    end

    command AddItem do |list, cmd|
      event ListStarted if list.status == :new
      event :item_added, name: cmd.payload.name
    end

    command :add_one, name: String do |_list, cmd|
      event :item_added, name: cmd.payload.name
    end

    event ListStarted do |list, _event|
      list.status = :active
    end

    # Test that this block is returned after #decide
    # wrapperd as a Sourced::Actions::Sync
    # to be run within the same transaction as the append action
    sync do |command:, events:, state:|
      SyncTracer.calls << [command, events, state]
    end

    event :item_added, name: String do |list, event|
      list.items << event.payload
    end

    reaction ListStarted do |list, event|
      dispatch(Notify, item_count: list.items.size).to('different-stream')
      dispatch(Notify, item_count: list.items.size).at(Time.now + 30)
    end

    reaction :item_added do |list, event|
      dispatch(Notify, item_count: list.items.size)
    end
  end
end

RSpec.describe Sourced::Actor do
  describe '.handled_messages' do
    it 'returns commands and events to react to' do
      expect(TestDomain::TodoListActor.handled_messages).to match_array([
        TestDomain::AddItem,
        TestDomain::TodoListActor::AddOne,
        TestDomain::ListStarted,
        TestDomain::TodoListActor::ItemAdded,
      ])
    end
  end

  describe '.command' do
    it 'raises if the message is also reacted to' do
      klass = Class.new(described_class)
      klass.reaction TestDomain::ListStarted do |_|
      end
      expect {
        klass.command TestDomain::ListStarted do |_, _|
        end
      }.to raise_error(Sourced::Actor::DualMessageRegistrationError)
    end
  end

  describe '.reaction' do
    it 'raises if the message is also registered as a command' do
      klass = Class.new(described_class)
      klass.command TestDomain::ListStarted do |_, _|
      end
      expect {
        klass.reaction TestDomain::ListStarted do |_|
        end
      }.to raise_error(Sourced::Actor::DualMessageRegistrationError)
    end
  end

  describe '#evolve' do
    it 'evolves internal state' do
      actor = TestDomain::TodoListActor.new
      e1 = TestDomain::ListStarted.parse(stream_id: actor.id, seq: 1)
      e2 = TestDomain::TodoListActor::ItemAdded.parse(
        stream_id: actor.id,
        seq: 2,
        payload: { name: 'Shoes' }
      )
      state = actor.evolve([e1, e2])
      expect(state).to eq(actor.state)
      expect(actor.state.status).to eq(:active)
      expect(actor.seq).to eq(2)
    end
  end

  describe '#decide' do
    it 'returns events with the right sequence, updates state' do
      actor = TestDomain::TodoListActor.new
      cmd = TestDomain::AddItem.parse(
        stream_id: actor.id,
        payload: { name: 'Shoes' }
      )
      events = actor.decide(cmd)
      expect(events.map(&:class)).to eq([TestDomain::ListStarted, TestDomain::TodoListActor::ItemAdded])
      expect(events.map(&:seq)).to eq([1, 2])
      expect(actor.seq).to eq(2)
      expect(actor.state.items.size).to eq(1)
      cmd2 = cmd.with(seq: 1)
      events = actor.decide(cmd2)
      expect(actor.seq).to eq(3)
      expect(events.map(&:class)).to eq([TestDomain::TodoListActor::ItemAdded])
      expect(events.map(&:seq)).to eq([3])
      expect(actor.state.items.size).to eq(2)
    end
  end

  describe '#react' do
    it 'reacts to events and return commands' do
      now = Time.now
      Timecop.freeze(now) do
        actor = TestDomain::TodoListActor.new
        event = TestDomain::ListStarted.parse(stream_id: actor.id)
        commands = actor.react(event)
        expect(commands.map(&:class)).to eq([TestDomain::Notify, TestDomain::Notify])
        expect(commands.first.metadata[:producer]).to eq('TestDomain::TodoListActor')
        expect(commands.map(&:created_at)).to eq([now, now + 30])
        expect(commands.map(&:stream_id)).to eq(['different-stream', actor.id])
      end
    end
  end

  describe '.handle' do
    context 'with a command to decide on' do
      let(:cmd) do
        TestDomain::AddItem.parse(
          stream_id: Sourced.new_stream_id, 
          seq: 1, 
          metadata: { foo: 'bar' },
          payload: { name: 'Shoes' }
        )
      end

      it 'returns an array with Sourced::Actions::AppendAfter Sourced::Actions::Sync actions' do
        result = TestDomain::TodoListActor.handle(cmd, history: [cmd])
        expect(result.map(&:class)).to eq([Sourced::Actions::AppendAfter, Sourced::Actions::Sync])
      end

      specify 'the AppendAfter action contains messages to append' do
        result = TestDomain::TodoListActor.handle(cmd, history: [cmd])
        append_action = result[0]
        expect(append_action.stream_id).to eq(cmd.stream_id)
        # two new events, seq 2, and 3
        expect(append_action.messages.map(&:seq)).to eq([2, 3])
        append_action.messages[0].tap do |msg|
          expect(msg).to be_a(TestDomain::ListStarted)
          expect(msg.stream_id).to eq(cmd.stream_id)
          expect(msg.metadata[:foo]).to eq('bar')
        end
        append_action.messages[1].tap do |msg|
          expect(msg).to be_a(TestDomain::TodoListActor::ItemAdded)
          expect(msg.stream_id).to eq(cmd.stream_id)
          expect(msg.metadata[:foo]).to eq('bar')
        end
      end

      specify 'the Sync action contains a side-effect to run' do
        result = TestDomain::TodoListActor.handle(cmd, history: [cmd])
        append_action = result[0]
        sync_action = result[1]

        expect(TestDomain::SyncTracer.calls).to eq([])
        sync_action.call
        expect(TestDomain::SyncTracer.calls.size).to eq(1)
        TestDomain::SyncTracer.calls.first.tap do |(command, events, state)|
          expect(command).to eq(cmd)
          expect(events).to eq(append_action.messages)
          expect(state).to be_a(TestDomain::TodoList)
          expect(state.items.size).to eq(1)
        end
      end
    end

    context 'with an event to react to' do
      let(:stream_id) { Sourced.new_stream_id }

      let(:history) do
        [
          TestDomain::AddItem.parse(stream_id:, seq: 1, payload: { name: 'test' }),
          TestDomain::ListStarted.parse(stream_id:, seq: 2),
          TestDomain::TodoListActor::ItemAdded.parse(stream_id:, seq: 3, payload: { name: 'test' })
        ]
      end

      it 'returns new commands to append' do
        result = TestDomain::TodoListActor.handle(history.last, history:)
        expect(result).to be_a(Array)
        expect(result.first).to be_a(Sourced::Actions::AppendNext)
        expect(result.first.messages.map(&:stream_id)).to eq([stream_id])
        expect(result.first.messages.map(&:class)).to eq([TestDomain::Notify])
        expect(result.first.messages.first.payload.item_count).to eq(1)
      end

      it 'returns multiple commands to append or schedule' do
        now = Time.now
        Timecop.freeze(now) do
          result = TestDomain::TodoListActor.handle(history[1], history:)
          expect(result).to be_a(Array)
          expect(result.map(&:class)).to eq [Sourced::Actions::AppendNext, Sourced::Actions::Schedule]
          expect(result[0].messages.size).to eq(1)
          result[0].messages[0].tap do |msg|
            expect(msg).to be_a TestDomain::Notify
            expect(msg.stream_id).to eq('different-stream')
            expect(msg.payload.item_count).to eq(1)
          end
          expect(result[1].messages.size).to eq(1)
          expect(result[1].at).to eq(now + 30)
          result[1].messages[0].tap do |msg|
            expect(msg).to be_a TestDomain::Notify
            expect(msg.stream_id).to eq(stream_id)
            expect(msg.payload.item_count).to eq(1)
          end
        end
      end
    end
  end
end
