require 'spec_helper'

RSpec.describe Sourced::CommandHandler do
  module HTE
    CreateUser = Sourced::Event.define('create_user') do
      field(:name).type(:string).present
      field(:age).type(:integer).present
    end
    UpdateUser = Sourced::Event.define('update_user') do
      field(:name).type(:string).present
      field(:age).type(:integer).present
    end
    UserCreated = Sourced::Event.define('users.created')
    NameChanged = Sourced::Event.define('users.name_changed') do
      field(:name).type(:string)
    end
    AgeChanged = Sourced::Event.define('users.age_changed') do
      field(:age).type(:integer)
    end

    User = Class.new do
      include Sourced::Aggregate
      attr_reader :name, :age
      on HTE::UserCreated do |evt|
        @id = evt.aggregate_id
      end
      on HTE::NameChanged do |evt|
        @name = evt.name
      end
      on HTE::AgeChanged do |evt|
        @age = evt.age
      end
    end
  end

  subject(:user_handler) {
    Class.new(Sourced::CommandHandler) do
      aggregates HTE::User

      on HTE::CreateUser do |cmd, user|
        user.apply HTE::UserCreated, aggregate_id: cmd.aggregate_id
        user.apply HTE::NameChanged, name: cmd.name
        user.apply HTE::AgeChanged, age: cmd.age
      end

      on HTE::UpdateUser do |cmd, user|

      end
    end
  }

  describe '#topics' do
    it 'list all handled topics' do
      expect(user_handler.topics).to eq %w(create_user update_user)
    end
  end

  describe '#call(command)' do
    it 'handles command and gathers events' do
      id = Sourced.uuid
      cmd = HTE::CreateUser.instance(
        aggregate_id: id,
        name: 'Ismael',
        age: 40,
      )
      user, events = user_handler.call(cmd)
      expect(user.id).to eq id
      expect(user.name).to eq 'Ismael'
      expect(events.size).to eq 3
      expect(events.map(&:topic)).to eq %w(users.created users.name_changed users.age_changed)
      expect(events.map(&:aggregate_id).uniq).to eq [id]
      expect(events.map(&:version)).to eq [1, 2, 3]
      expect(events.map(&:parent_id).uniq).to eq [cmd.id]
    end
  end
end
