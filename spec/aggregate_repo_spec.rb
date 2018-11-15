require 'spec_helper'

RSpec.describe Sourced::AggregateRepo do
  let(:event_store) { Sourced::MemEventStore.new }
  let(:aggregate_id) { Sourced.uuid }
  subject(:repo) { described_class.new(event_store: event_store) }

  describe '#add' do
    it 'registers aggregate without loading history' do
      event_store.append([
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
      ])

      user = repo.add(aggregate_id, UserDomain::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to be nil
      expect(user.version).to eq 0
    end
  end

  describe '#load' do
    it 'loads new aggregate when no events available' do
      user = repo.load(aggregate_id, UserDomain::User)
      expect(user.id).to eq aggregate_id
      # loads same new user again, even if it's new
      user2 = repo.load(aggregate_id, UserDomain::User)
      expect(user.object_id).to eq user.object_id
    end

    it 'loads from available history' do
      event_store.append([
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user = repo.load(aggregate_id, UserDomain::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.version).to eq 3

      # it loads cached aggregate
      expect(event_store).not_to receive(:by_aggregate_id)
      user2 = repo.load(aggregate_id, UserDomain::User)
      expect(user).to eq user2
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

    it 'catches up with new changes if :catchup' do
      event_store.append([
        UserDomain::UserCreated.new!(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        UserDomain::NameChanged.new!(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
      ])

      user = repo.load(aggregate_id, UserDomain::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 39
      expect(user.version).to eq 2

      event_store.append([
        UserDomain::AgeChanged.new!(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user2 = repo.load(aggregate_id, UserDomain::User, catchup: true)
      expect(user2).to eq user
      expect(user2.id).to eq aggregate_id
      expect(user2.name).to eq 'Mr. Ismael'
      expect(user2.age).to eq 40
      expect(user2.version).to eq 3
    end
  end

  describe '#clear_events' do
    it 'collects aggregate events, in the order they were applied across aggregates' do
      id1 = Sourced.uuid
      id2 = Sourced.uuid
      user1 = repo.load(id1, UserDomain::User)
      user2 = repo.load(id2, UserDomain::User)

      user1.apply UserDomain::UserCreated, aggregate_id: id1, name: 'Ismael', age: 39
      user1.apply UserDomain::NameChanged, name: 'Ismael'
      user2.apply UserDomain::UserCreated, aggregate_id: id2, name: 'Joe', age: 42
      user1.apply UserDomain::AgeChanged, age: 40

      # expect(user1.events.map(&:topic)).to eq %w(users.created users.name.changed users.age.changed)
      # expect(user2.events.map(&:topic)).to eq %w(users.created)

      events = repo.clear_events
      # events will be in order aggregates where loaded, not in order they where applied to aggregates
      # is this a problem?
      # expect(events.map(&:topic)).to eq %w(users.created users.name.changed users.age.changed users.created)
      expect(events.map(&:topic)).to eq %w(users.created users.name.changed users.created users.age.changed)
      expect(events.map(&:aggregate_id)).to eq [user1.id, user1.id, user2.id, user1.id]
      # expect(events.map(&:aggregate_id)).to eq [user1.id, user1.id, user1.id, user2.id]
    end
  end
end
