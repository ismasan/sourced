require 'spec_helper'

RSpec.describe Sourced::AggregateRepo do
  let(:event_store) { Sourced::MemEventStore.new }
  let(:aggregate_id) { Sourced.uuid }
  subject(:repo) { described_class.new(event_store: event_store) }

  describe '#load' do
    it 'loads from available history' do
      event_store.append([
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, seq: 1, name: 'Ismael', age: 39),
        UserDomain::UserCreated.new!(aggregate_id: Sourced.uuid, seq: 1, name: 'Someone Else', age: 20),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, seq: 2, name: 'Mr. Ismael'),
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, seq: 3, age: 40),
      ])

      user = repo.load(aggregate_id, UserDomain::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.seq).to eq 3
    end

    it 'loads up to a specific event id' do
      events = [
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, seq: 1, name: 'Ismael', age: 39),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, seq: 2, name: 'Mr. Ismael'),
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, seq: 3, age: 40),
      ]

      event_store.append(events)

      user = repo.load(aggregate_id, UserDomain::User, upto: events[1].id)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 39
      expect(user.events.size).to eq 0
      expect(user.seq).to eq 2
    end

    it 'loads from a specific event id on' do
      events = [
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, seq: 1, name: 'Ismael', age: 39),
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, seq: 2, age: 40),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, seq: 3, name: 'Mr. Ismael'),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, seq: 4, name: 'Mr. Joe'),
      ]

      event_store.append(events)

      user = repo.load(aggregate_id, UserDomain::User, from: events[2].id)
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
      stream = event_store.by_aggregate_id(user.id)
      expect(stream.map(&:topic)).to eq %w(users.created users.name.changed)
    end
  end

  describe '#persist_events' do
    it 'persists aggregate events into the event store' do
      id = Sourced.uuid
      repo.persist_events([UserDomain::UserCreated.new!(aggregate_id: id, name: 'Foo', age: 41)])
      stream = event_store.by_aggregate_id(id)
      expect(stream.map(&:topic)).to eq %w(users.created)
    end
  end
end
