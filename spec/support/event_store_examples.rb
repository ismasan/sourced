RSpec.shared_examples_for 'an event store' do
  describe '#append' do
    it 'appends events and retrieves events by aggregate_id' do
      id1 = Sourced.uuid
      id2 = Sourced.uuid
      e1 = UserDomain::UserCreated.new!(aggregate_id: id1, name: 'Ismael', age: 40)
      e2 = UserDomain::UserCreated.new!(aggregate_id: id2, name: 'Joe', age: 42)
      e3 = UserDomain::NameChanged.new!(aggregate_id: id1, name: 'Ismael jr.')

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
