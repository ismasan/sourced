# frozen_string_literal: true

require 'spec_helper'

module ProjectorTest
  STORE = {}

  State = Struct.new(:id, :total)

  Added = Sourced::Event.define('prtest.added') do
    attribute :amount, Integer
  end

  Probed = Sourced::Event.define('prtest.probed')

  NextCommand = Sourced::Command.define('prtest.next_command') do
    attribute :amount, Integer
  end

  NextCommand2 = Sourced::Command.define('prtest.next_command2') do
    attribute :amount, Integer
  end

  class StateStored < Sourced::Projector::StateStored
    state do |id|
      STORE[id] || State.new(id, 0)
    end

    event Added do |state, event|
      state.total += event.payload.amount
    end

    sync do |state, _, _events|
      STORE[state.id] = state
    end
  end

  class EventSourced < Sourced::Projector::EventSourced
    state do |id|
      State.new(id, 0)
    end

    event Added do |state, event|
      state.total += event.payload.amount
    end

    sync do |state, _, events|
      STORE[state.id] = [state, events.last.type]
    end
  end

  class StateStoredWithReactions < Sourced::Projector::StateStored
    state do |id|
      STORE[id] || State.new(id, 0)
    end

    event Added do |state, event|
      state.total += event.payload.amount
    end

    event Probed # register so that it's handled by .reaction_with_state

    # React to a specific event
    reaction Added do |state, event|
      if state.total > 20
        stream_for(event).command NextCommand, amount: state.total
      end
    end

    # React to any event
    reaction do |state, event|
      if state.total > 10
        stream_for(event).command NextCommand2, amount: state.total
      end
    end

    sync do |state, _, _events|
      STORE[state.id] = state
    end
  end
end

RSpec.describe Sourced::Projector do
  before do
    ProjectorTest::STORE.clear
  end

  describe Sourced::Projector::StateStored do
    it 'has consumer info' do
      expect(ProjectorTest::StateStored.consumer_info.group_id).to eq('ProjectorTest::StateStored')
    end

    specify 'with new state' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      ProjectorTest::StateStored.handle_events([e1, e2], replaying: false)

      expect(ProjectorTest::STORE['111'].total).to eq(15)
    end

    specify 'with existing state' do
      ProjectorTest::STORE['111'] = ProjectorTest::State.new('111', 10)

      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      ProjectorTest::StateStored.handle_events([e1, e2], replaying: false)

      expect(ProjectorTest::STORE['111'].total).to eq(25)
    end
  end

  describe 'Sourced::Projector::StateStored with reactions' do
    it 'reacts to events based on projected state, and returns commands' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 5 })
      e3 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 6 })

      commands = ProjectorTest::StateStoredWithReactions.handle_events([e1, e2], replaying: false)
      expect(commands).to eq([])

      commands = ProjectorTest::StateStoredWithReactions.handle_events([e3], replaying: false)
      expect(ProjectorTest::STORE['222'].total).to eq(21)
      expect(commands.map(&:class)).to eq([ProjectorTest::NextCommand])
      expect(commands.map(&:stream_id)).to eq(['222'])
      expect(commands.first.payload.amount).to eq(21)
    end

    it 'reacts to wildcard events, if it evolves from them' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 12 })
      e2 = ProjectorTest::Probed.parse(stream_id: '222')

      commands = ProjectorTest::StateStoredWithReactions.handle_events([e1, e2], replaying: false)
      expect(commands.map(&:class)).to eq([ProjectorTest::NextCommand2])
    end

    it 'does not react if replaying' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 22 })

      commands = ProjectorTest::StateStoredWithReactions.handle_events([e1], replaying: true)
      expect(commands).to eq([])
      expect(ProjectorTest::STORE['222'].total).to eq(22)
    end

    it 'rejects reactions to events not handled by .event handlers' do
      expect {
        Class.new(Sourced::Projector::StateStored) do
          reaction ProjectorTest::Added do |_state, _event|
          end
        end
      }.to raise_error(ArgumentError)
    end
  end

  describe Sourced::Projector::EventSourced do
    it 'has consumer info' do
      expect(ProjectorTest::EventSourced.consumer_info.group_id).to eq('ProjectorTest::EventSourced')
    end

    specify 'with no previous events' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      result = ProjectorTest::EventSourced.handle_events([e1, e2], replaying: false)

      expect(result).to eq([])
      obj, last_event_type = ProjectorTest::STORE['111']
      expect(obj.total).to eq(15)
      expect(last_event_type).to eq('prtest.added')
    end

    specify 'with previous events' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', seq: 1, payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', seq: 2, payload: { amount: 5 })
      e3 = ProjectorTest::Added.parse(stream_id: '111', seq: 3, payload: { amount: 7 })

      Sourced.config.backend.append_to_stream('111', [e1])

      result = ProjectorTest::EventSourced.handle_events([e2, e3], replaying: false)

      expect(result).to eq([])
      obj, last_event_type = ProjectorTest::STORE['111']
      expect(obj.total).to eq(22)
      expect(last_event_type).to eq('prtest.added')
    end
  end
end
