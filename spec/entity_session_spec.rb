# frozen_string_literal: true

require 'spec_helper'
require 'sourced/entity_session'

RSpec.describe Sourced::EntitySession do

  class UserProjector < Sourced::Projector
    on UserDomain::UserCreated do |event, user|
      user[:name] = event.payload.name
      user[:age] = event.payload.age
    end
    on UserDomain::AgeChanged do |event, user|
      user[:age] = event.payload.age
    end
    on UserDomain::NameChanged do |event, user|
      user[:name] = event.payload.name
    end
  end

  module UserEntity
    def self.call(id)
      {
        id: id,
        name: '',
        age: 0
      }
    end
  end

  let(:id) { Sourced.uuid }
  let(:e1) { UserDomain::UserCreated.new!(entity_id: id, seq: 1, payload: { name: 'Joe', age: 41 }) }
  let(:e2) { UserDomain::NameChanged.new!(entity_id: id, seq: 2, payload: { name: 'Ismael' }) }
  let(:e3) { UserDomain::AgeChanged.new!(entity_id: id, seq: 3, payload: { age: 42 }) }

  shared_examples_for 'an EntitySession' do
    describe '.load(id, stream)' do
      it 'instantiates entity and projects state from stream' do
        stream = [e1, e2, e3]

        user = session_constructor.load(id, stream)
        expect(user.seq).to eq 3
        expect(user.entity[:id]).to eq id
        expect(user.entity[:name]).to eq 'Ismael'
        expect(user.entity[:age]).to eq 42
      end
    end

    describe '#apply and #events' do
      it 'applies new events, projects new state and collects events' do
        stream = [e1, e2, e3]

        user = session_constructor.load(id, stream)

        user.apply(UserDomain::NameChanged, payload: { name: 'Ismael 2' })
        user.apply(UserDomain::AgeChanged, payload: { age: 43 })

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

        user = session_constructor.load(id, stream)

        user.apply(UserDomain::NameChanged, payload: { name: 'Ismael 2' })
        user.apply(UserDomain::AgeChanged, payload: { age: 43 })

        user.clear_events.tap do |events|
          expect(events.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])
          expect(events.map(&:seq)).to eq([4, 5])
        end
        expect(user.events.size).to eq(0)
      end
    end
  end

  context 'with inline entity and projector' do
    let(:session_constructor) do
      Class.new(Sourced::EntitySession) do
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
            user[:name] = event.payload.name
            user[:age] = event.payload.age
          end
          on UserDomain::AgeChanged do |event, user|
            user[:age] = event.payload.age
          end
          on UserDomain::NameChanged do |event, user|
            user[:name] = event.payload.name
          end
        end
      end
    end

    it_behaves_like 'an EntitySession'
  end

  context 'with inline entity and shared projector' do
    let(:session_constructor) do
      Class.new(Sourced::EntitySession) do
        entity do |id|
          {
            id: id,
            name: '',
            age: 0
          }
        end

        projector UserProjector
      end
    end

    it_behaves_like 'an EntitySession'
  end

  context 'with shared entity constructor' do
    let(:session_constructor) do
      Class.new(Sourced::EntitySession) do
        entity UserEntity
        projector UserProjector
      end
    end

    it_behaves_like 'an EntitySession'
  end
end
