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

    sync do |state:, events:, replaying:|
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

    sync do |state:, events:, replaying:|
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

    event Probed # register so that it's handled by .reaction

    # React to a specific event
    reaction Added do |state, event|
      if state.total > 20
        dispatch(NextCommand, amount: state.total).to(event)
      end
    end

    # React to any event
    reaction do |state, event|
      if state.total > 10
        dispatch(NextCommand2, amount: state.total)
      end
    end

    sync do |state:, events:, replaying:|
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

      actions = ProjectorTest::StateStored.handle(e1, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
      # Run actions. Normally the backend runs these
      run_sync_blocks(actions)

      expect(ProjectorTest::STORE['111'].total).to eq(10)
    end

    specify 'with existing state' do
      ProjectorTest::STORE['111'] = ProjectorTest::State.new('111', 10)

      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })

      actions = ProjectorTest::StateStored.handle(e1, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
      # Run actions. Normally the backend runs these
      run_sync_blocks(actions)

      expect(ProjectorTest::STORE['111'].total).to eq(20)
    end

    it 'increments @seq' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', seq: 1, payload: { amount: 12 })
      e2 = ProjectorTest::Probed.parse(stream_id: '222', seq: 2)
      projector = ProjectorTest::StateStored.new(id: '222')
      projector.evolve([e1, e2])
      expect(projector.seq).to eq(2)
    end
  end

  describe 'Sourced::Projector::StateStored with reactions' do
    it 'reacts to events based on projected state, and returns commands' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 5 })
      e3 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 6 })

      actions = ProjectorTest::StateStoredWithReactions.handle(e1, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
      run_sync_blocks(actions)

      actions = ProjectorTest::StateStoredWithReactions.handle(e1, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
      run_sync_blocks(actions)

      actions = ProjectorTest::StateStoredWithReactions.handle(e2, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync, Sourced::Actions::AppendNext])
      run_sync_blocks(actions)

      actions = ProjectorTest::StateStoredWithReactions.handle(e3, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync, Sourced::Actions::AppendNext])
      run_sync_blocks(actions)
      expect(ProjectorTest::STORE['222'].total).to eq(31)
      expect(actions.last.messages.map(&:class)).to eq([ProjectorTest::NextCommand])
      expect(actions.last.messages.map(&:stream_id)).to eq(['222'])
      expect(actions.last.messages.first.payload.amount).to eq(31)
    end

    it 'reacts to wildcard events, if it evolves from them' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 12 })
      e2 = ProjectorTest::Probed.parse(stream_id: '222')

      actions = ProjectorTest::StateStoredWithReactions.handle(e1, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
      run_sync_blocks(actions)
      actions = ProjectorTest::StateStoredWithReactions.handle(e2, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync, Sourced::Actions::AppendNext])
      expect(actions.last.messages.map(&:class)).to eq([ProjectorTest::NextCommand2])
    end

    it 'does not react if replaying' do
      e1 = ProjectorTest::Added.parse(stream_id: '222', payload: { amount: 12 })
      e2 = ProjectorTest::Probed.parse(stream_id: '222')

      actions = ProjectorTest::StateStoredWithReactions.handle(e1, replaying: false)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
      run_sync_blocks(actions)
      actions = ProjectorTest::StateStoredWithReactions.handle(e2, replaying: true)
      expect(actions.map(&:class)).to eq([Sourced::Actions::Sync])
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

    specify 'it builds state from history, returns sync action to persist it' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      # In Sourced's arch, the new message is included in the history
      actions = ProjectorTest::EventSourced.handle(e2, replaying: false, history: [e1, e2])
      run_sync_blocks(actions)

      obj, last_event_type = ProjectorTest::STORE['111']
      expect(obj.total).to eq(15)
      expect(last_event_type).to eq('prtest.added')
    end
  end

  private def run_sync_blocks(actions)
    actions.filter{ |a| a.is_a?(Sourced::Actions::Sync) }.each(&:call)
  end
end
