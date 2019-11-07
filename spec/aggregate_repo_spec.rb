require 'spec_helper'

RSpec.describe Sourced::AggregateRepo do
  let(:event_store) { Sourced::MemEventStore.new }
  let(:aggregate_id) { Sourced.uuid }
  subject(:repo) { described_class.new(event_store: event_store) }

  describe '#load' do
    it 'loads from available history' do
      event_store.append([
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        UserDomain::UserCreated.new!(aggregate_id: Sourced.uuid, version: 1, name: 'Someone Else', age: 20),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user = repo.load(aggregate_id, UserDomain::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.version).to eq 3
    end

    it 'loads up to a specific version' do
      event_store.append([
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user = repo.load(aggregate_id, UserDomain::User, upto: 2)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 39
      expect(user.events.size).to eq 0
      expect(user.version).to eq 2
    end
  end

  describe '#build' do
    it 'builds a new aggregate with a UUID' do
      user = repo.build(UserDomain::User)
      expect(user.id).not_to be_nil
    end
  end

  describe '#persist' do
    it 'persists aggregate events into the event store' do
      user = repo.build(UserDomain::User)
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
