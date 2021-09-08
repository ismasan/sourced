# frozen_string_literal: true

require 'sourced/rspec_helpers'

RSpec.shared_examples_for 'an event store' do
  let(:id1) { Sourced.uuid }
  let(:id2) { Sourced.uuid }
  let(:e1) { Sourced::UserDomain::UserCreated.new(entity_id: id1, seq: 1, payload: { name: 'Ismael', age: 40 }) }
  let(:e2) { Sourced::UserDomain::UserCreated.new(entity_id: id2, seq: 1, payload: { name: 'Joe', age: 42 }) }
  let(:e3) { Sourced::UserDomain::NameChanged.new(entity_id: id1, seq: 2, payload: { name: 'Ismael jr.' }) }
  let(:e4) { Sourced::UserDomain::NameChanged.new(entity_id: id1, seq: 3, payload: { name: 'Ismael sr.' }) }

  it_behaves_like 'a Sourced event store' do
    subject(:event_store) { store }
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
