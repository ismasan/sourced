# frozen_string_literal: true

require 'spec_helper'
require 'sourced/projector'

RSpec.describe Sourced::Projector do
  it 'registers event handlers and responds to .call' do
    id = Sourced.uuid

    projector = Class.new(described_class) do
      on Sourced::UserDomain::UserCreated do |user, event|
        user[:seq] = event.seq
        user[:name] = event.payload.name
        user[:age] = event.payload.age
      end
      on Sourced::UserDomain::NameChanged do |user, event|
        user[:seq] = event.seq
        user[:name] = event.payload.name
      end
    end

    e1 = Sourced::UserDomain::UserCreated.new(stream_id: id, seq: 1, payload: { name: 'Joe', age: 41 })
    e2 = Sourced::UserDomain::NameChanged.new(stream_id: id, seq: 2, payload: { name: 'Ismael' })

    user = { name: 'Foo', age: 20 }
    projector.call(user, e1)
    projector.call(user, e2)

    expect(user[:name]).to eq('Ismael')
    expect(user[:age]).to eq(41)
    expect(user[:seq]).to eq(2)
  end
end
