require 'spec_helper'

RSpec.describe Sourced::CommandHandler do
  describe '#topics' do
    it 'list all handled topics' do
      expect(UserDomain::UserHandler.topics).to eq %w(users.create users.update)
    end
  end

  describe '#call(command)' do
    it 'handles command and gathers events' do
      id = Sourced.uuid
      cmd = UserDomain::CreateUser.instance(
        aggregate_id: id,
        name: 'Ismael',
        age: 40,
      )
      user, events = UserDomain::UserHandler.call(cmd)
      expect(user.id).to eq id
      expect(user.name).to eq 'Ismael'
      expect(events.size).to eq 3
      expect(events.map(&:topic)).to eq %w(users.created users.name.changed users.age.changed)
      expect(events.map(&:aggregate_id).uniq).to eq [id]
      expect(events.map(&:version)).to eq [1, 2, 3]
      expect(events.map(&:parent_id).uniq).to eq [cmd.id]
    end
  end
end
