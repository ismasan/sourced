require 'spec_helper'

RSpec.describe Sourced::AggregateRepo do
  module ARE
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

  let(:event_store) { Sourced::MemEventStore.new }
  let(:aggregate_id) { Sourced.uuid }
  subject(:repo) { described_class.new(event_store: event_store) }

  describe '#load' do
    it 'loads new aggregate when no events available' do
      user = repo.load(aggregate_id, ARE::User)
      expect(user.id).to be nil
    end

    it 'loads from available history' do
      event_store.append([
        ARE::UserCreated.instance(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        ARE::NameChanged.instance(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
        ARE::AgeChanged.instance(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user = repo.load(aggregate_id, ARE::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 40
      expect(user.version).to eq 3

      # it loads cached aggregate
      expect(event_store).not_to receive(:by_aggregate_id)
      user2 = repo.load(aggregate_id, ARE::User)
      expect(user).to eq user2
    end

    it 'loads up to a specific version' do
      event_store.append([
        ARE::UserCreated.instance(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        ARE::NameChanged.instance(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
        ARE::AgeChanged.instance(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user = repo.load(aggregate_id, ARE::User, upto: 2)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 39
      expect(user.events.size).to eq 0
      expect(user.version).to eq 2
    end

    it 'catches up with new changes if :catchup' do
      event_store.append([
        ARE::UserCreated.instance(aggregate_id: aggregate_id, version: 1, name: 'Ismael', age: 39),
        ARE::NameChanged.instance(aggregate_id: aggregate_id, version: 2, name: 'Mr. Ismael'),
      ])

      user = repo.load(aggregate_id, ARE::User)
      expect(user.id).to eq aggregate_id
      expect(user.name).to eq 'Mr. Ismael'
      expect(user.age).to eq 39
      expect(user.version).to eq 2

      event_store.append([
        ARE::AgeChanged.instance(aggregate_id: aggregate_id, version: 3, age: 40),
      ])

      user2 = repo.load(aggregate_id, ARE::User, catchup: true)
      expect(user2).to eq user
      expect(user2.id).to eq aggregate_id
      expect(user2.name).to eq 'Mr. Ismael'
      expect(user2.age).to eq 40
      expect(user2.version).to eq 3
    end
  end
end
