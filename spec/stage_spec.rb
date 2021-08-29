# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Stage do
  class UserProjector < Sourced::Projector
    on UserDomain::UserCreated do |user, event|
      user[:name] = event.payload.name
      user[:age] = event.payload.age
    end
    on UserDomain::AgeChanged do |user, event|
      user[:age] = event.payload.age
    end
    on UserDomain::NameChanged do |user, event|
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
  let(:e1) { UserDomain::UserCreated.new(entity_id: id, seq: 1, payload: { name: 'Joe', age: 41 }) }
  let(:e2) { UserDomain::NameChanged.new(entity_id: id, seq: 2, payload: { name: 'Ismael' }) }
  let(:e3) { UserDomain::AgeChanged.new(entity_id: id, seq: 3, payload: { age: 42 }) }

  shared_examples_for 'a Stage' do
    describe '.load(id, stream)' do
      it 'instantiates entity and projects state from stream' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)
        expect(user.seq).to eq 3
        expect(user.last_committed_seq).to eq 3
        expect(user.entity[:id]).to eq id
        expect(user.entity[:name]).to eq 'Ismael'
        expect(user.entity[:age]).to eq 42
      end
    end

    describe '#apply and #events' do
      it 'applies new events, projects new state and collects events' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)

        user.apply(UserDomain::NameChanged, payload: { name: 'Ismael 2' })
        user.apply(UserDomain::AgeChanged, payload: { age: 43 })

        expect(user.id).to eq id
        expect(user.last_committed_seq).to eq 3
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

    describe '#apply with event instance' do
      it 'still keeps track of seq' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)

        user.apply(UserDomain::NameChanged.new(payload: { name: 'Ismael 2' }))
        user.apply(UserDomain::AgeChanged.new(payload: { age: 43 }))

        expect(user.id).to eq id
        expect(user.last_committed_seq).to eq 3
        expect(user.seq).to eq 5
        expect(user.entity[:name]).to eq 'Ismael 2'
        expect(user.entity[:age]).to eq 43
        expect(user.events.size).to eq 2
        user.events.tap do |events|
          expect(events.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])
          expect(events.map(&:seq)).to eq([4, 5])
          expect(events.map(&:entity_id)).to eq([id, id])
        end
      end
    end

    describe '#commit' do
      it 'yields collected events and last committed seq, clears them from stage' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)

        user.apply(UserDomain::NameChanged, payload: { name: 'Ismael 2' })
        user.apply(UserDomain::AgeChanged, payload: { age: 43 })

        expect(user.seq).to eq 5
        expect(user.last_committed_seq).to eq 3

        called = false
        evts = user.commit do |seq, events, entity|
          called = true
          expect(seq).to eq(3)
          expect(events.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])
          expect(events.map(&:seq)).to eq([4, 5])
          expect(entity).to eq(user.entity)
        end

        expect(evts.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])

        expect(called).to be true
        # Updates stage state after committing
        expect(user.last_committed_seq).to eq 5
        expect(user.events.size).to eq(0)
      end
    end
  end

  context 'with inline entity and projector' do
    let(:stage_constructor) do
      Class.new(Sourced::Stage) do
        entity do |id|
          {
            id: id,
            name: '',
            age: 0
          }
        end

        # projector UserProjector
        projector do
          on UserDomain::UserCreated do |user, event|
            user[:name] = event.payload.name
            user[:age] = event.payload.age
          end
          on UserDomain::AgeChanged do |user, event|
            user[:age] = event.payload.age
          end
          on UserDomain::NameChanged do |user, event|
            user[:name] = event.payload.name
          end
        end
      end
    end

    it_behaves_like 'a Stage'
  end

  context 'with inline entity and shared projector' do
    let(:stage_constructor) do
      Class.new(Sourced::Stage) do
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

    it_behaves_like 'a Stage'
  end

  context 'with shared entity constructor' do
    let(:stage_constructor) do
      Class.new(Sourced::Stage) do
        entity UserEntity
        projector UserProjector
      end
    end

    it_behaves_like 'a Stage'
  end

  context 'with a simple callable projector' do
    let(:stage_constructor) do
      Class.new(Sourced::Stage) do
        entity UserEntity
        projector ->(user, evt) {
          case evt
          when UserDomain::UserCreated
            user.merge(evt.payload.to_h)
          when UserDomain::AgeChanged
            user.merge(evt.payload.to_h)
          when UserDomain::NameChanged
            user.merge(evt.payload.to_h)
          else
            user
          end
        }
      end
    end

    it_behaves_like 'a Stage'
  end
end
