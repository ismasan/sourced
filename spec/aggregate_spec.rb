require 'spec_helper'

RSpec.describe Sourced::Aggregate do
  describe '#load_from' do
    it 'loads state from event stream Enumerable' do
      id = Sourced.uuid
      stream = [
        UserDomain::UserCreated.new!(aggregate_id: id, name: 'Joe', age: 41),
        UserDomain::NameChanged.new!(aggregate_id: id, name: 'Joan')
      ]
      user = UserDomain::User.new(id)
      user.load_from(stream)

      expect(user.name).to eq 'Joan'
      expect(user.events.size).to eq 0
    end
  end

  describe '#apply' do
    it 'increments version and gathers events with aggregate id' do
      id = Sourced.uuid
      events = []
      user = UserDomain::User.new(id, events: events)
      user.start 'Ismael', 30
      user.name = 'Mr. Ismael'
      user.age = 40

      expect(user.id).to eq id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.version).to eq 3
      expect(user.events).to eq events
      expect(user.events.size).to eq 3

      expect(user.events[0].topic).to eq 'users.created'
      expect(user.events[0].aggregate_id).to eq id
      expect(user.events[0].version).to eq 1

      expect(user.events[1].topic).to eq 'users.name.changed'
      expect(user.events[1].aggregate_id).to eq id
      expect(user.events[1].version).to eq 2

      expect(user.events[2].topic).to eq 'users.age.changed'
      expect(user.events[2].aggregate_id).to eq id
      expect(user.events[2].version).to eq 3
    end

    it 'can define attributes to be added to all events' do
      evt = Sourced::Event.define('foobar') do
        field(:foo)
      end

      klass = Class.new(UserDomain::User) do
        on evt do |e|

        end

        private
        def basic_event_attrs
          {foo: 'bar'}
        end
      end

      user = klass.new(Sourced.uuid)
      user.apply evt
      expect(user.events.first.foo).to eq 'bar'
    end
  end
end
