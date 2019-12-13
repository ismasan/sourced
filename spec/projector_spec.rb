# frozen_string_literal: true

require 'spec_helper'
require 'sourced/entity_session'

RSpec.describe Sourced::Projector do
  it 'registers event handlers and responds to .call' do
    id = Sourced.uuid

    projector = Class.new(described_class) do
      on UserDomain::UserCreated do |event, user|
        user[:name] = event.name
        user[:age] = event.age
        user[:seq] = event.seq
      end
      on UserDomain::NameChanged do |event, user|
        user[:name] = event.name
        user[:seq] = event.seq
      end
    end

    e1 = UserDomain::UserCreated.new!(aggregate_id: id, name: 'Joe', age: 41, seq: 1)
    e2 = UserDomain::NameChanged.new!(aggregate_id: id, name: 'Ismael', seq: 2)

    user = { name: 'Foo', age: 20 }
    projector.call(e1, user)
    projector.call(e2, user)

    expect(user[:name]).to eq('Ismael')
    expect(user[:age]).to eq(41)
    expect(user[:seq]).to eq(2)
  end
end