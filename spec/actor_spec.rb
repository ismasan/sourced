# frozen_string_literal: true

require 'spec_helper'

module TestDomain
  TodoList = Struct.new(:archive_status, :seq, :id, :status, :items)

  AddItem = Sourced::Message.define('actor.todos.add') do
    attribute :name, String
  end

  Notify = Sourced::Message.define('actor.todos.notify')

  ListStarted = Sourced::Message.define('actor.todos.started')
  ArchiveRequested = Sourced::Message.define('actor.todos.archive_requested')
  ConfirmArchive = Sourced::Message.define('actor.todos.archive_confirm')
  ArchiveConfirmed = Sourced::Message.define('actor.todos.archive_confirmed')

  class TodoListActor < Sourced::Actor
    state do
      TodoList.new(nil, 0, nil, :new, [])
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

    event :item_added, name: String do |list, event|
      list.items << event.payload
    end

    reaction ListStarted do |event, state|
    end

    reaction :item_added do |event|
      stream_for(event).command Notify
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

      it 'returns an Sourced::Actions::AppendAfter action with events to append' do
        result = TestDomain::TodoListActor.handle(cmd, history: [cmd])
        expect(result).to be_a(Sourced::Actions::AppendAfter)
        expect(result.stream_id).to eq(cmd.stream_id)
        # two new events, seq 2, and 3
        expect(result.messages.map(&:seq)).to eq([2, 3])
        result.messages[0].tap do |msg|
          expect(msg).to be_a(TestDomain::ListStarted)
          expect(msg.stream_id).to eq(cmd.stream_id)
          expect(msg.metadata[:foo]).to eq('bar')
        end
        result.messages[1].tap do |msg|
          expect(msg).to be_a(TestDomain::TodoListActor::ItemAdded)
          expect(msg.stream_id).to eq(cmd.stream_id)
          expect(msg.metadata[:foo]).to eq('bar')
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
        expect(result).to be_a(Sourced::Actions::AppendNext)
        expect(result.messages.map(&:stream_id)).to eq([stream_id])
        expect(result.messages.map(&:class)).to eq([TestDomain::Notify])
      end
    end
  end
end
