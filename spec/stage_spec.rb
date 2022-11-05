# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Stage do
  class UserProjector < Sourced::Projector
    on Sourced::UserDomain::UserCreated do |user, event|
      user[:name] = event.payload.name
      user[:age] = event.payload.age
    end
    on Sourced::UserDomain::AgeChanged do |user, event|
      user[:age] = event.payload.age
    end
    on Sourced::UserDomain::NameChanged do |user, event|
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
  let(:e1) { Sourced::UserDomain::UserCreated.new(stream_id: id, seq: 1, payload: { name: 'Joe', age: 41 }) }
  let(:e2) { Sourced::UserDomain::NameChanged.new(stream_id: id, seq: 2, payload: { name: 'Ismael' }) }
  let(:e3) { Sourced::UserDomain::AgeChanged.new(stream_id: id, seq: 3, payload: { age: 42 }) }

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

        user.apply(Sourced::UserDomain::NameChanged, payload: { name: 'Ismael 2' })
        user.apply(Sourced::UserDomain::AgeChanged, payload: { age: 43 })

        expect(user.id).to eq id
        expect(user.last_committed_seq).to eq 3
        expect(user.seq).to eq 5
        expect(user.entity[:name]).to eq 'Ismael 2'
        expect(user.entity[:age]).to eq 43
        expect(user.events.size).to eq 2
        user.events.tap do |events|
          expect(events.map(&:class)).to eq([Sourced::UserDomain::NameChanged, Sourced::UserDomain::AgeChanged])
          expect(events.map(&:seq)).to eq([4, 5])
        end
      end
    end

    describe '#apply with event instance' do
      it 'still keeps track of seq' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)

        user.apply(Sourced::UserDomain::NameChanged.new(payload: { name: 'Ismael 2' }))
        user.apply(Sourced::UserDomain::AgeChanged.new(payload: { age: 43 }))

        expect(user.id).to eq id
        expect(user.last_committed_seq).to eq 3
        expect(user.seq).to eq 5
        expect(user.entity[:name]).to eq 'Ismael 2'
        expect(user.entity[:age]).to eq 43
        expect(user.events.size).to eq 2
        user.events.tap do |events|
          expect(events.map(&:class)).to eq([Sourced::UserDomain::NameChanged, Sourced::UserDomain::AgeChanged])
          expect(events.map(&:seq)).to eq([4, 5])
          expect(events.map(&:stream_id)).to eq([id, id])
        end
      end
    end

    describe '#with_event_decorator' do
      it 'returns copy with new event decorator in decorator chain' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)

        user.apply(Sourced::UserDomain::NameChanged.new(payload: { name: 'Ismael 2' }))

        decorator = ->(attrs) { attrs.merge(metadata: { foo: 'bar' })}
        user = user.with_event_decorator(decorator)
        user.apply(Sourced::UserDomain::AgeChanged.new(payload: { age: 43 }))
        expect(user.events.map(&:metadata)).to eq([{}, { foo: 'bar' }])
        expect(user.events.map(&:seq)).to eq([4, 5])
        expect(user.id).to eq id
        expect(user.last_committed_seq).to eq 3
        expect(user.seq).to eq 5
        expect(user.entity[:name]).to eq 'Ismael 2'
        expect(user.entity[:age]).to eq 43
      end
    end

    describe '#with_metadata' do
      it 'adds metadata to all applied events' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream).with_metadata(foo: 'bar')
        # with event instance
        user.apply(Sourced::UserDomain::NameChanged.new(metadata: { year: 2022 }, payload: { name: 'Ismael 2' }))
        # with event constructor
        user.apply(Sourced::UserDomain::AgeChanged, metadata: { lol: 'cats' }, payload: { age: 43 })
        expect(user.events.map(&:metadata)).to eq([{ year: 2022, foo: 'bar' }, { lol: 'cats', foo: 'bar' }])
      end
    end

    describe '#commit' do
      it 'yields collected events and last committed seq, clears them from stage' do
        stream = [e1, e2, e3]

        user = stage_constructor.load(id, stream)

        user.apply(Sourced::UserDomain::NameChanged, payload: { name: 'Ismael 2' })
        user.apply(Sourced::UserDomain::AgeChanged, payload: { age: 43 })

        expect(user.seq).to eq 5
        expect(user.last_committed_seq).to eq 3

        called = false
        evts = user.commit do |seq, events, entity|
          called = true
          expect(seq).to eq(3)
          expect(events.map(&:class)).to eq([Sourced::UserDomain::NameChanged, Sourced::UserDomain::AgeChanged])
          expect(events.map(&:seq)).to eq([4, 5])
          expect(entity).to eq(user.entity)
        end

        expect(evts.map(&:class)).to eq([Sourced::UserDomain::NameChanged, Sourced::UserDomain::AgeChanged])

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
          on Sourced::UserDomain::UserCreated do |user, event|
            user[:name] = event.payload.name
            user[:age] = event.payload.age
          end
          on Sourced::UserDomain::AgeChanged do |user, event|
            user[:age] = event.payload.age
          end
          on Sourced::UserDomain::NameChanged do |user, event|
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
          when Sourced::UserDomain::UserCreated
            user.merge(evt.payload.to_h)
          when Sourced::UserDomain::AgeChanged
            user.merge(evt.payload.to_h)
          when Sourced::UserDomain::NameChanged
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
