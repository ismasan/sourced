require 'spec_helper'

RSpec.describe Sourced::Aggregate do
  module AGT
    UserCreated = Sourced::Event.define('users.created') do
      field(:name).type(:string).present
      field(:age).type(:integer).present
    end
    NameChanged = Sourced::Event.define('users.name.changed') do
      field(:name).type(:string).present
    end
    AgeChanged = Sourced::Event.define('users.age.changed') do
      field(:age).type(:integer).present
    end

    User = Class.new do
      include Sourced::Aggregate
      attr_reader :id, :name, :age
      def initialize
        @id = nil
        @name = nil
        @age = nil
      end

      def start(id, name, age)
        emit UserCreated, aggregate_id: id, name: name, age: age
      end

      def name=(n)
        emit NameChanged, name: n
      end

      def age=(a)
        emit AgeChanged, age: a
      end

      on UserCreated do |evt|
        @id = evt.aggregate_id
        @name = evt.name
        @age = evt.age
      end
      on NameChanged do |evt|
        @name = evt.name
      end
      on AgeChanged do |evt|
        @age = evt.age
      end
    end
  end

  describe '#emit' do
    it 'increments version and gathers events with aggregate id' do
      id = Sourced.uuid
      user = AGT::User.new
      user.start id, 'Ismael', 30
      user.name = 'Mr. Ismael'
      user.age = 40

      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.version).to eq 3
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
  end
end
