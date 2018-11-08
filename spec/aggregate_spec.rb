require 'spec_helper'

RSpec.describe Sourced::Aggregate do
  describe '#apply' do
    it 'increments version and gathers events with aggregate id' do
      id = Sourced.uuid
      user = UserDomain::User.new
      user.start id, 'Ismael', 30
      user.name = 'Mr. Ismael'
      user.age = 40

      expect(user.id).to eq id
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
