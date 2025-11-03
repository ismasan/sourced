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

  it 'loads history and evolves actor' do
    actor = actor_class.new(id: stream_id)

    e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
    e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

    Sourced.config.backend.append_to_stream(stream_id, [e1, e2])
    actor = Sourced.load(actor)
    expect(actor.seq).to eq(2)
    expect(actor.state[:age]).to eq(40)
  end

  it 'catches up to latest history' do
    actor = actor_class.new(id: stream_id)

    e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
    e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

    Sourced.config.backend.append_to_stream(stream_id, [e1])
    actor = Sourced.load(actor)
    expect(actor.seq).to eq(1)

    Sourced.config.backend.append_to_stream(stream_id, [e2])
    actor = Sourced.load(actor)
    expect(actor.seq).to eq(2)
  end

  it 'loads events up to a given sequence' do
    actor = actor_class.new(id: stream_id)

    e1 = actor_class[:start].parse(stream_id:, seq: 1, payload: { name: 'Joe' })
    e2 = actor_class[:update_age].parse(stream_id:, seq: 2, payload: { age: 40 })

    Sourced.config.backend.append_to_stream(stream_id, [e1, e2])

    actor = Sourced.load(actor, upto: 1)
    expect(actor.seq).to eq(1)
  end
end
