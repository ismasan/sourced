# frozen_string_literal: true

require 'spec_helper'
require 'sourced/entity_session'

RSpec.describe Sourced::Projector do
  it 'registers event handlers and responds to .call' do
    id = Sourced.uuid

    projector = Class.new(described_class) do
      on UserDomain::UserCreated do |event, user|
        user[:seq] = event.seq
        user[:name] = event.payload.name
        user[:age] = event.payload.age
      end
      on UserDomain::NameChanged do |event, user|
        user[:seq] = event.seq
        user[:name] = event.payload.name
      end
    end

    e1 = UserDomain::UserCreated.new!(entity_id: id, seq: 1, payload: { name: 'Joe', age: 41 })
    e2 = UserDomain::NameChanged.new!(entity_id: id, seq: 2, payload: { name: 'Ismael' })

    user = { name: 'Foo', age: 20 }
    projector.call(e1, user)
    projector.call(e2, user)

    expect(user[:name]).to eq('Ismael')
    expect(user[:age]).to eq(41)
    expect(user[:seq]).to eq(2)
  end
end
