require 'spec_helper'

RSpec.describe Sourced::AggregateRepo do
  let(:event_store) { Sourced::MemEventStore.new }
  let(:entity_id) { Sourced.uuid }
  subject(:repo) { described_class.new(event_store: event_store) }

  describe '#load' do
    it 'loads from available history' do
      event_store.append([
        UserDomain::UserCreated.new!(entity_id: entity_id, seq: 1, payload: { name: 'Ismael', age: 39 }),
        UserDomain::UserCreated.new!(entity_id: Sourced.uuid, seq: 1, payload: { name: 'Someone Else', age: 20 }),
        UserDomain::NameChanged.new!(entity_id: entity_id, seq: 2, payload: { name: 'Mr. Ismael' }),
        UserDomain::AgeChanged.new!(entity_id: entity_id, seq: 3, payload: { age: 40 }),
      ])

      user = repo.load(entity_id, UserDomain::User)
      expect(user.id).to eq entity_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.seq).to eq 3
    end

    it 'loads up to a specific event id' do
      events = [
        UserDomain::UserCreated.new!(entity_id: entity_id, seq: 1, payload: { name: 'Ismael', age: 39 }),
        UserDomain::NameChanged.new!(entity_id: entity_id, seq: 2, payload: { name: 'Mr. Ismael' }),
        UserDomain::AgeChanged.new!(entity_id: entity_id, seq: 3, payload: { age: 40 }),
      ]

      event_store.append(events)

      user = repo.load(entity_id, UserDomain::User, upto: events[1].id)
      expect(user.id).to eq entity_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 39
      expect(user.events.size).to eq 0
      expect(user.seq).to eq 2
    end

    it 'loads from a specific event id on' do
      events = [
        UserDomain::UserCreated.new!(entity_id: entity_id, seq: 1, payload: { name: 'Ismael', age: 39 }),
        UserDomain::AgeChanged.new!(entity_id: entity_id, seq: 2, payload: { age: 40 }),
        UserDomain::NameChanged.new!(entity_id: entity_id, seq: 3, payload: { name: 'Mr. Ismael' }),
        UserDomain::NameChanged.new!(entity_id: entity_id, seq: 4, payload: { name: 'Mr. Joe' }),
      ]

      event_store.append(events)

      user = repo.load(entity_id, UserDomain::User, from: events[2].id)
      expect(user.age).to be nil
      expect(user.seq).to eq 4
    end
  end

  describe '#persist' do
    it 'persists aggregate events into the event store' do
      user = UserDomain::User.build
      user.start('Ismael', 41)
      user.name = 'Joe'
      repo.persist(user)
      stream = event_store.by_entity_id(user.id)
      expect(stream.map(&:topic)).to eq %w(users.created users.name.changed)
    end
  end

  describe '#persist_events' do
    it 'persists aggregate events into the event store' do
      id = Sourced.uuid
      repo.persist_events([UserDomain::UserCreated.new!(entity_id: id, payload: { name: 'Foo', age: 41 })])
      stream = event_store.by_entity_id(id)
      expect(stream.map(&:topic)).to eq %w(users.created)
    end
  end
end
