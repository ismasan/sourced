# frozen_string_literal: true

require 'spec_helper'
require 'sourced/entity_session'

RSpec.describe Sourced::EntitySession do

  # class UserProjector < Projector
  #   on UserDomain::UserCreated do |event, user|
  #     user[:name] = event.name
  #     user[:age] = event.age
  #   end
  #   on UserDomain::AgeChanged do |event, user|
  #     user[:age] = event.age
  #   end
  #   on UserDomain::NameChanged do |event, user|
  #     user[:name] = event.name
  #   end
  # end

  # User = EntitySession.define do
  #   entity do |id|
  #     {
  #       id: id,
  #       name: '',
  #       age: 0
  #     }
  #   end

  #   # projection UserProjector
  #   projection do
  #     on UserDomain::UserCreated do |event, user|
  #       user[:name] = event.name
  #       user[:age] = event.age
  #     end
  #     on UserDomain::AgeChanged do |event, user|
  #       user[:age] = event.age
  #     end
  #     on UserDomain::NameChanged do |event, user|
  #       user[:name] = event.name
  #     end
  #   end
  # end

  class UserSession < Sourced::EntitySession
    entity do |id|
      {
        id: id,
        name: '',
        age: 0
      }
    end

    # projector UserProjector
    projector do
      on UserDomain::UserCreated do |event, user|
        user[:name] = event.name
        user[:age] = event.age
      end
      on UserDomain::AgeChanged do |event, user|
        user[:age] = event.age
      end
      on UserDomain::NameChanged do |event, user|
        user[:name] = event.name
      end
    end
  end

  let(:id) { Sourced.uuid }
  let(:e1) { UserDomain::UserCreated.new!(aggregate_id: id, name: 'Joe', age: 41, seq: 1) }
  let(:e2) { UserDomain::NameChanged.new!(aggregate_id: id, name: 'Ismael', seq: 2) }
  let(:e3) { UserDomain::AgeChanged.new!(aggregate_id: id, age: 42, seq: 3) }

  describe '.load(id, stream)' do
    it 'instantiates entity and projects state from stream' do
      stream = [e1, e2, e3]

      user = UserSession.load(id, stream)
      expect(user.seq).to eq 3
      expect(user.entity[:id]).to eq id
      expect(user.entity[:name]).to eq 'Ismael'
      expect(user.entity[:age]).to eq 42
    end
  end

  describe '#apply and #events' do
    it 'applies new events, projects new state and collects events' do
      stream = [e1, e2, e3]

      user = UserSession.load(id, stream)

      user.apply(UserDomain::NameChanged, name: 'Ismael 2')
      user.apply(UserDomain::AgeChanged, age: 43)

      expect(user.id).to eq id
      expect(user.seq).to eq 5
      expect(user.entity[:name]).to eq 'Ismael 2'
      expect(user.entity[:age]).to eq 43
      expect(user.events.size).to eq 2
      user.events.tap do |events|
        expect(events.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])
        expect(events.map(&:seq)).to eq([4, 5])
      end
    end
  end

  describe '#clear_events' do
    it 'returns collected events and clear them from session' do
      stream = [e1, e2, e3]

      user = UserSession.load(id, stream)

      user.apply(UserDomain::NameChanged, name: 'Ismael 2')
      user.apply(UserDomain::AgeChanged, age: 43)

      user.clear_events.tap do |events|
        expect(events.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])
        expect(events.map(&:seq)).to eq([4, 5])
      end
      expect(user.events.size).to eq(0)
    end
  end
end
