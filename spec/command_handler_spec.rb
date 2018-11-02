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
  end

  subject(:user_handler) {
    Class.new(Sourced::CommandHandler) do
      on HTE::CreateUser do |cmd|
        emit HTE::UserCreated
        emit HTE::NameChanged, name: cmd.name # will instantiate
        emit HTE::AgeChanged.instance(age: cmd.age)
      end

      on HTE::UpdateUser do |cmd|

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
      cmd = HTE::CreateUser.instance(
        name: 'Ismael',
        age: 40,
      )
      events = user_handler.call(cmd)
      expect(events.size).to eq 3
      expect(events.map(&:topic)).to eq %w(users.created users.name_changed users.age_changed)
    end
  end
end
