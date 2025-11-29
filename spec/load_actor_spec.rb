# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Sourced.load' do
  let(:stream_id) { 'abc' }

  let(:actor_class) do
    Class.new(Sourced::Actor) do
      state do |id|
        { id:, name: nil, age: 0 }
      end

      event :start, name: String do |state, event|
        state[:name] = event.payload.name
      end

      event :update_age, age: Integer do |state, event|
        state[:age] = event.payload.age
      end
    end
  end

  before do
    Sourced.config.backend.clear!
  end

  describe '.load' do
    it 'loads history and evolves actor' do
      actor = actor_class.new(id: stream_id)

      e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
      e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

      Sourced.config.backend.append_to_stream(stream_id, [e1, e2])
      actor, events = Sourced.load(actor)
      expect(actor.seq).to eq(2)
      expect(actor.state[:age]).to eq(40)
      expect(events.map(&:id)).to eq([e1.id, e2.id])
    end

    it 'load from class and stream_id' do
      e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
      e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

      Sourced.config.backend.append_to_stream(stream_id, [e1, e2])

      actor, events = Sourced.load(actor_class, stream_id)
      expect(actor).to be_a(actor_class)
      expect(actor.seq).to eq(2)
      expect(actor.state[:age]).to eq(40)
      expect(events.map(&:id)).to eq([e1.id, e2.id])
    end

    it 'catches up to latest history' do
      actor = actor_class.new(id: stream_id)

      e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
      e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

      Sourced.config.backend.append_to_stream(stream_id, [e1])
      actor, events = Sourced.load(actor)
      expect(actor.seq).to eq(1)
      expect(events.map(&:id)).to eq([e1.id])

      Sourced.config.backend.append_to_stream(stream_id, [e2])
      actor, events = Sourced.load(actor)
      expect(actor.seq).to eq(2)
      expect(events.map(&:id)).to eq([e2.id])
    end

    it 'loads events up to a given sequence' do
      actor = actor_class.new(id: stream_id)

      e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
      e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

      Sourced.config.backend.append_to_stream(stream_id, [e1, e2])

      actor, events = Sourced.load(actor, upto: 1)
      expect(actor.seq).to eq(1)
      expect(events.map(&:id)).to eq([e1.id])
    end
  end

  describe '.history_for' do
    it 'loads events for an #id interface' do
      actor = actor_class.new(id: stream_id)

      e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
      e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

      Sourced.config.backend.append_to_stream(stream_id, [e1, e2])

      history = Sourced.history_for(actor)
      expect(history.map(&:class)).to eq([actor_class[:start], actor_class[:update_age]])
      expect(history.map(&:id)).to eq([e1.id, e2.id])

      history = Sourced.history_for(actor, upto: 1)
      expect(history.map(&:class)).to eq([actor_class[:start]])
      expect(history.map(&:id)).to eq([e1.id])
    end
  end
end
