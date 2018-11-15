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
      cmd = UserDomain::CreateUser.new!(
        aggregate_id: id,
        name: 'Ismael',
        age: 40,
      )
      user, events = UserDomain::UserHandler.call(cmd)
      expect(user.id).to eq id
      expect(user.name).to eq 'Ismael'
      expect(events.size).to eq 4
      expect(events.map(&:topic)).to eq %w(users.create users.created users.name.changed users.age.changed)
      expect(events.map(&:aggregate_id).uniq).to eq [id]
      expect(events.map(&:version)).to eq [1, 1, 2, 3]
      expect(events.map(&:parent_id).uniq).to eq [nil, cmd.id] # command itself doesn't have parent id
    end
  end

  # context 'calling another handler from within a handler' do
  #   CreateAccount = Sourced::Event.define('accounts.create')
  #   AccountCreated = Sourced::Event.define('accounts.created')

  #   it 'works' do
  #     account_handler = Class.new(Sourced::CommandHandler) do
  #       on CreateAccount do |cmd|
  #         # generate events of its own
  #         apply AccountCreated, aggregate_id: cmd.aggregate_id
  #         # call other handlers, pass the repository
  #         UserDomain::UserHandler.call(UserDomain::CreateUser.new!(
  #           aggregate_id: Sourced.uuid,
  #           name: 'Ismael',
  #           age: 40
  #         ))
  #       end
  #     end
  #   end
  # end
end
