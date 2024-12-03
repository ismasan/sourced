# frozen_string_literal: true

require 'spec_helper'

module ProjectorTest
  STORE = {}

  State = Struct.new(:id, :total)

  Added = Sourced::Event.define('prtest.added') do
    attribute :amount, Integer
  end

  class StateStored < Sourced::Projector::StateStored
    def init_state(id)
      STORE[id] || State.new(id, 0)
    end

    evolve Added do |state, event|
      state.total += event.payload.amount
    end

    sync do |state, _, _events|
      STORE[state.id] = state
    end
  end

  class EventSourced < Sourced::Projector::EventSourced
    def init_state(id)
      State.new(id, 0)
    end

    evolve Added do |state, event|
      state.total += event.payload.amount
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
    specify 'with no previous events' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', payload: { amount: 5 })

      result = ProjectorTest::EventSourced.handle_events([e1, e2])

      expect(result).to eq([])
      expect(ProjectorTest::STORE['111'].total).to eq(15)
    end

    specify 'with previous events' do
      e1 = ProjectorTest::Added.parse(stream_id: '111', seq: 1, payload: { amount: 10 })
      e2 = ProjectorTest::Added.parse(stream_id: '111', seq: 2, payload: { amount: 5 })
      e3 = ProjectorTest::Added.parse(stream_id: '111', seq: 3, payload: { amount: 7 })

      Sourced.config.backend.append_to_stream('111', [e1])

      result = ProjectorTest::EventSourced.handle_events([e2, e3])

      expect(result).to eq([])
      expect(ProjectorTest::STORE['111'].total).to eq(22)
    end
  end
end
