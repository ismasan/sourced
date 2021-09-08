# frozen_string_literal: true

require_relative '../../spec/support/user_domain'

module Sourced
  RSpec.shared_examples 'a valid Sourced event' do
    let(:event) { event_constructor.new(attributes) }

    specify '.topic' do
      expect(event_constructor.topic).to be_a(String)
    end

    specify '#id' do
      expect(event.id).not_to be_nil
    end

    specify '#entity_id' do
      expect(event.entity_id).not_to be_nil
    end

    specify '#date' do
      expect(event.date).not_to be_nil
    end

    specify '#to_h' do
      hash = event.to_h
      expect(hash[:topic]).to eq(event_constructor.topic)
      attributes.each do |k, v|
        expect(hash[k]).to eq(v)
      end
    end

    specify '#copy' do
      new_entity_id = Sourced.uuid
      event2 = event.copy(entity_id: new_entity_id)
      expect(event2).to be_a(event.class)
      expect(event2.entity_id).to eq(new_entity_id)
      expect(event2.id).to eq(event.id)
    end
  end

  RSpec.shared_examples_for 'a Sourced event store' do
    let(:id1) { Sourced.uuid }
    let(:id2) { Sourced.uuid }
    let(:e1) { Sourced::UserDomain::UserCreated.new(entity_id: id1, seq: 1, payload: { name: 'Ismael', age: 40 }) }
    let(:e2) { Sourced::UserDomain::UserCreated.new(entity_id: id2, seq: 1, payload: { name: 'Joe', age: 42 }) }
    let(:e3) { Sourced::UserDomain::NameChanged.new(entity_id: id1, seq: 2, payload: { name: 'Ismael jr.' }) }
    let(:e4) { Sourced::UserDomain::NameChanged.new(entity_id: id1, seq: 3, payload: { name: 'Ismael sr.' }) }

    describe '#append and #by_entity_id' do
      it 'appends events and retrieves events by entity_id' do
        evts = event_store.append(e1)
        expect(evts).to eq [e1]

        evts = event_store.append([e2, e3])
        expect(evts).to eq [e2, e3]

        evts = event_store.by_entity_id(id1)
        expect(evts.map(&:id)).to eq [e1.id, e3.id]

        evts = event_store.by_entity_id(id2)
        expect(evts.map(&:id)).to eq [e2.id]
      end

      it 'blows up if passed unexpected sequence' do
        event_store.append([e1, e2, e3, e4])
        e5 = UserDomain::NameChanged.new(entity_id: id1, seq: 4, payload: { name: 'nope' })
        expect {
          event_store.append(e5, expected_seq: e3.seq)
        }.to raise_error(Sourced::ConcurrencyError)
      end

      it 'is a noop if empty events list' do
        evts = event_store.append([])
        expect(evts).to eq([])
      end
    end

    describe '#by_entity_id' do
      it 'supports :upto_seq argument' do
        event_store.append([e1, e2, e3, e4])

        stream = event_store.by_entity_id(id1, upto_seq: 2)
        expect(stream.map(&:id)).to eq [e1.id, e3.id]
      end

      it 'supports :upto argument' do
        event_store.append([e1, e2, e3, e4])

        stream = event_store.by_entity_id(id1, upto: e3.id)
        expect(stream.map(&:id)).to eq [e1.id, e3.id]
      end

      it 'supports :after argument' do
        event_store.append([e1, e2, e3, e4])

        stream = event_store.by_entity_id(id1, after: e1.id)
        expect(stream.map(&:id)).to eq [e3.id, e4.id]
      end
    end

    describe '#transaction' do
      it 'yields' do
        evts = []
        event_store.transaction do
          evts = event_store.append([e1, e2])
        end

        expect(evts).to eq [e1, e2]
      end
    end
  end
end
