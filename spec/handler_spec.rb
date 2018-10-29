require 'spec_helper'

RSpec.describe Sourced::Handler do
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

  let(:user_handler) {
    Class.new do
      include Sourced::Handler

      on CreateUser do |cmd|
        emit UserCreated.instance
        emit NameChanged.instance(name: cmd.name)
        emit AgeChanged.instance(age: cmd.age)
      end

      on UpdateUser do |cmd|

      end
    end
  }
  subject(:handler) { user_handler.new }

  describe '#topics' do
    it 'list all handled topics' do
      expect(handler.topics).to eq %w(create_user update_user)
    end
  end

  describe '#call' do
    it 'handles command and gathers events' do
      cmd = CreateUser.instance(
        name: 'Ismael',
        age: 40,
      )
      events = handler.call(cmd)
      expect(events.size).to eq 4
      expect(events.map(&:topic)).to eq %w(create_user users.created users.name_changed users.age_changed)
    end
  end
end
