RSpec.shared_examples_for 'an event store' do
  let(:id1) { Sourced.uuid }
  let(:id2) { Sourced.uuid }
  let(:e1) { UserDomain::UserCreated.new!(entity_id: id1, seq: 1, payload: { name: 'Ismael', age: 40 }) }
  let(:e2) { UserDomain::UserCreated.new!(entity_id: id2, seq: 1, payload: { name: 'Joe', age: 42 }) }
  let(:e3) { UserDomain::NameChanged.new!(entity_id: id1, seq: 2, payload: { name: 'Ismael jr.' }) }
  let(:e4) { UserDomain::NameChanged.new!(entity_id: id1, seq: 3, payload: { name: 'Ismael sr.' }) }

  describe '#append and #entity_id' do
    it 'appends events and retrieves events by entity_id' do
      evts = store.append(e1)
      expect(evts).to eq [e1]

      evts = store.append([e2, e3])
      expect(evts).to eq [e2, e3]

      evts = store.by_entity_id(id1)
      expect(evts.map(&:id)).to eq [e1.id, e3.id]

      evts = store.by_entity_id(id2)
      expect(evts.map(&:id)).to eq [e2.id]
    end

    it 'blows up if passed unexpected sequence' do
      store.append([e1, e2, e3, e4])
      e5 = UserDomain::NameChanged.new!(entity_id: id1, seq: 4, payload: { name: 'nope' })
      expect {
        store.append(e5, expected_seq: e3.seq)
      }.to raise_error(Sourced::ConcurrencyError)
    end
  end

  describe '#by_entity_id' do
    it 'supports :upto argument' do
      store.append([e1, e2, e3, e4])

      stream = store.by_entity_id(id1, upto: e3.id)
      expect(stream.map(&:id)).to eq [e1.id, e3.id]
    end

    it 'supports :from argument' do
      store.append([e1, e2, e3, e4])

      stream = store.by_entity_id(id1, from: e3.id)
      expect(stream.map(&:id)).to eq [e3.id, e4.id]
    end
  end

  describe '#stream' do
    let(:events) { [e1, e2, e3, e4] }
    before do
      store.append(events)
    end

    it 'returns an Enumerator' do
      expect(store.stream).to be_a Enumerator
      expect(store.stream.map(&:id)).to eq events.map(&:id)
    end

    it 'supports :entity_id argument' do
      stream = store.stream(entity_id: id1)
      expect(stream.map(&:id)).to eq [e1, e3, e4].map(&:id)
    end

    it 'supports :from argument' do
      stream = store.stream(from: Sourced.uuid)
      expect(stream.map(&:id)).to eq []

      stream = store.stream(from: e2.id)
      expect(stream.map(&:id)).to eq [e2, e3, e4].map(&:id)
    end

    it 'supports :upto argument' do
      stream = store.stream(upto: Sourced.uuid)
      expect(stream.map(&:id)).to eq []

      stream = store.stream(upto: e3.id)
      expect(stream.map(&:id)).to eq [e1, e2, e3].map(&:id)
    end

    it 'combines filters' do
      stream = store.stream(entity_id: id1, from: e2.id)
      expect(stream.map(&:id)).to eq []

      stream = store.stream(entity_id: id1, from: e3.id)
      expect(stream.map(&:id)).to eq [e3, e4].map(&:id)
    end
  end
end
