# frozen_string_literal: true

require 'spec_helper'

module ProjectorTest
  STORE = {}

  State = Struct.new(:id, :total)

  Added = Sourced::Event.define('prtest.added') do
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

      ProjectorTest::StateStored.handle_events([e1, e2])

      expect(ProjectorTest::STORE['111'].total).to eq(15)
    end

    specify 'with existing state' do
      ProjectorTest::STORE['111'] = ProjectorTest::State.new('111', 10)

      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      ProjectorTest::StateStored.handle_events([e1, e2])

      expect(ProjectorTest::STORE['111'].total).to eq(25)
    end
  end

  describe Sourced::Projector::EventSourced do
    it 'has consumer info' do
      expect(ProjectorTest::EventSourced.consumer_info.group_id).to eq('ProjectorTest::EventSourced')
    end

    specify 'with no previous events' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      result = ProjectorTest::EventSourced.handle_events([e1, e2])

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

      result = ProjectorTest::EventSourced.handle_events([e2, e3])

      expect(result).to eq([])
      obj, last_event_type = ProjectorTest::STORE['111']
      expect(obj.total).to eq(22)
      expect(last_event_type).to eq('prtest.added')
    end
  end
end
