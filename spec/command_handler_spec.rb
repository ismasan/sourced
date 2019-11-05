require 'spec_helper'

RSpec.describe Sourced::CommandHandler do
  describe '#topics' do
    it 'list all handled topics' do
      expect(UserDomain::UserHandler.new.topics).to eq %w(users.create users.update)
    end
  end

  specify '#aggregate_class' do
    expect(UserDomain::UserHandler.aggregate_class).to eq UserDomain::User
    expect(UserDomain::UserHandler.new.aggregate_class).to eq UserDomain::User
  end

  describe '#call(command, aggregate)' do
    it 'handles command and gathers events' do
      id = Sourced.uuid
      user = UserDomain::User.new(id: id)
      cmd = UserDomain::CreateUser.new!(
        aggregate_id: id,
        name: 'Ismael',
        age: 40,
      )
      user, events = UserDomain::UserHandler.new.call(cmd, user)
      expect(user.id).to eq id
      expect(user.name).to eq 'Ismael'
      expect(events.size).to eq 3
      expect(events.map(&:topic)).to eq %w(users.created users.name.changed users.age.changed)
      expect(events.map(&:aggregate_id).uniq).to eq [id]
      expect(events.map(&:version)).to eq [1, 2, 3]
    end
  end
end
