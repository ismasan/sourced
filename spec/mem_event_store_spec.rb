require 'spec_helper'

RSpec.describe Sourced::MemEventStore do
  subject(:store) { described_class.new }

  describe '#append' do
    it 'appends events that can be retrieved' do
      id1 = Sourced.uuid
      id2 = Sourced.uuid
      e1 = UserDomain::UserCreated.instance(aggregate_id: id1, name: 'Ismael', age: 40)
      e2 = UserDomain::UserCreated.instance(aggregate_id: id2, name: 'Joe', age: 42)
      e3 = UserDomain::NameChanged.instance(aggregate_id: id1, name: 'Ismael jr.')

      evts = store.append(e1)
      expect(evts).to eq [e1]

      evts = store.append([e2, e3])
      expect(evts).to eq [e2, e3]

      evts = store.by_aggregate_id(id1)
      expect(evts.map(&:id)).to eq [e1.id, e3.id]

      evts = store.by_aggregate_id(id2)
      expect(evts.map(&:id)).to eq [e2.id]
    end
  end
end
