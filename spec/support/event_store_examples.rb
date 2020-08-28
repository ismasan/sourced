# frozen_string_literal: true

RSpec.shared_examples_for 'an event store' do
  let(:id1) { Sourced.uuid }
  let(:id2) { Sourced.uuid }
  let(:e1) { UserDomain::UserCreated.new!(entity_id: id1, seq: 1, payload: { name: 'Ismael', age: 40 }) }
  let(:e2) { UserDomain::UserCreated.new!(entity_id: id2, seq: 1, payload: { name: 'Joe', age: 42 }) }
  let(:e3) { UserDomain::NameChanged.new!(entity_id: id1, seq: 2, payload: { name: 'Ismael jr.' }) }
  let(:e4) { UserDomain::NameChanged.new!(entity_id: id1, seq: 3, payload: { name: 'Ismael sr.' }) }

  describe '#append and #by_entity_id' do
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

    it 'is a noop if empty events list' do
      evts = store.append([])
      expect(evts).to eq([])
    end
  end

  describe '#by_entity_id' do
    it 'supports :upto_seq argument' do
      store.append([e1, e2, e3, e4])

      stream = store.by_entity_id(id1, upto_seq: 2)
      expect(stream.map(&:id)).to eq [e1.id, e3.id]
    end

    it 'supports :upto argument' do
      store.append([e1, e2, e3, e4])

      stream = store.by_entity_id(id1, upto: e3.id)
      expect(stream.map(&:id)).to eq [e1.id, e3.id]
    end

    it 'supports :after argument' do
      store.append([e1, e2, e3, e4])

      stream = store.by_entity_id(id1, after: e1.id)
      expect(stream.map(&:id)).to eq [e3.id, e4.id]
    end
  end

  describe '#transaction' do
    it 'yields' do
      evts = []
      store.transaction do
        evts = store.append([e1, e2])
      end

      expect(evts).to eq [e1, e2]
    end
  end

  describe '#filter' do
    let(:events) { [e1, e2, e3, e4] }
    before do
      store.append(events)
    end

    it 'returns an Enumerator' do
      expect(store.filter).to be_a Enumerator
      expect(store.filter.map(&:id)).to eq events.map(&:id)
    end

    it 'supports :entity_id argument' do
      stream = store.filter(entity_id: id1)
      expect(stream.map(&:id)).to eq [e1, e3, e4].map(&:id)
    end

    it 'supports :after argument' do
      stream = store.filter(after: Sourced.uuid)
      expect(stream.map(&:id)).to eq []

      stream = store.filter(after: e2.id)
      expect(stream.map(&:id)).to eq [e3, e4].map(&:id)
    end

    it 'supports :upto argument' do
      stream = store.filter(upto: Sourced.uuid)
      expect(stream.map(&:id)).to eq []

      stream = store.filter(upto: e3.id)
      expect(stream.map(&:id)).to eq [e1, e2, e3].map(&:id)
    end

    it 'combines filters' do
      stream = store.filter(entity_id: id1, after: e2.id)
      expect(stream.map(&:id)).to eq []

      stream = store.filter(entity_id: id1, after: e3.id)
      expect(stream.map(&:id)).to eq [e4].map(&:id)
    end
  end
end
