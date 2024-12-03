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
end
