require 'spec_helper'

RSpec.describe Sourced::Aggregate do
  describe '.build' do
    it 'builds new with generated uuid' do
      user = UserDomain::User.build
      expect(user.id).not_to be nil
    end

    it 'builds with passed uuid' do
      uuid = Sourced.uuid
      user = UserDomain::User.build(uuid)
      expect(user.id).to eq(uuid)
    end
  end

  describe '#load_from' do
    it 'loads state from event stream Enumerable' do
      id = Sourced.uuid
      stream = [
        UserDomain::UserCreated.new!(entity_id: id, payload: { name: 'Joe', age: 41 }),
        UserDomain::NameChanged.new!(entity_id: id, payload: { name: 'Joan' })
      ]
      user = UserDomain::User.new(id)
      user.load_from(stream)

      expect(user.name).to eq 'Joan'
      expect(user.last_event_id).to eq(stream.last.id)
      expect(user.events.size).to eq 0
    end
  end

  describe '#==' do
    it 'works on current state' do
      id = Sourced.uuid
      stream = [
        UserDomain::UserCreated.new!(entity_id: id, payload: { name: 'Joe', age: 41 }),
        UserDomain::NameChanged.new!(entity_id: id, payload: { name: 'Joan' })
      ]
      user1 = UserDomain::User.new(id)
      user1.load_from(stream)
      user2 = UserDomain::User.new(id)
      user2.load_from(stream)
      user3 = UserDomain::User.new(id)
      user3.load_from([stream.first])

      expect(user1 == user2).to be true
      expect(user1 == user3).to be false
    end
  end

  describe '#apply' do
    it 'increments #seq and gathers events with aggregate id' do
      id = Sourced.uuid
      user = UserDomain::User.new(id)
      user.start 'Ismael', 30
      user.name = 'Mr. Ismael'
      user.age = 40

      expect(user.id).to eq id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.seq).to eq 3
      expect(user.events.size).to eq 3

      expect(user.events[0].topic).to eq 'users.created'
      expect(user.events[0].entity_id).to eq id
      expect(user.events[0].seq).to eq 1

      expect(user.events[1].topic).to eq 'users.name.changed'
      expect(user.events[1].entity_id).to eq id
      expect(user.events[1].seq).to eq 2

      expect(user.events[2].topic).to eq 'users.age.changed'
      expect(user.events[2].entity_id).to eq id
      expect(user.events[2].seq).to eq 3
    end
  end

  context 'when setup as self-persisting' do
    let(:event_store) { Sourced::MemEventStore.new }
    let(:repo) { Sourced::AggregateRepo.new(event_store: event_store) }
    let!(:user_class) do
      Class.new(Sourced::Aggregate) do
        include Sourced::Persistable

        attr_reader :name, :age

        on UserDomain::UserCreated do |evt|
          @id = evt.entity_id
          @name = evt.payload.name
          @age = evt.payload.age
        end

        on UserDomain::NameChanged do |evt|
          @name = evt.payload.name
        end

        on UserDomain::AgeChanged do |evt|
          @age = evt.payload.age
        end
      end
    end

    before do
      user_class.repository(repo)
    end

    it 'includes .build, .load and #persist to manage state from event store' do
      user = user_class.build
      user.apply UserDomain::UserCreated, payload: { name: 'Ismael', age: 41 }
      user.apply UserDomain::NameChanged, payload: { name: 'Joe' }

      user.persist

      expect(user.name).to eq 'Joe'

      user2 = user_class.load(user.id)
      expect(user.object_id).not_to eq(user2.object_id)
      expect(user2.name).to eq 'Joe'
      stream = event_store.by_entity_id(user.id)
      expect(stream.map(&:topic)).to eq(%w(users.created users.name.changed))
    end
  end
end
