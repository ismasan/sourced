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

    specify '#stream_id' do
      expect(event.stream_id).not_to be_nil
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
      new_stream_id = Sourced.uuid
      event2 = event.copy(stream_id: new_stream_id)
      expect(event2).to be_a(event.class)
      expect(event2.stream_id).to eq(new_stream_id)
      expect(event2.id).to eq(event.id)
    end
  end

  RSpec.shared_examples_for 'a Sourced event store' do
    let(:stream_id1) { Sourced.uuid }
    let(:stream_id2) { Sourced.uuid }
    let(:e1) { Sourced::UserDomain::UserCreated.new(stream_id: stream_id1, seq: 1, payload: { name: 'Ismael', age: 40 }) }
    let(:e2) { Sourced::UserDomain::UserCreated.new(stream_id: stream_id2, seq: 1, payload: { name: 'Joe', age: 42 }) }
    let(:e3) { Sourced::UserDomain::NameChanged.new(stream_id: stream_id1, seq: 2, payload: { name: 'Ismael jr.' }) }
    let(:e4) { Sourced::UserDomain::NameChanged.new(stream_id: stream_id1, seq: 3, payload: { name: 'Ismael sr.' }) }

    describe '#append_to_stream and #read_stream' do
      it 'appends events and retrieves events by stream_id' do
        evts = event_store.append_to_stream(stream_id1, e1)
        expect(evts).to eq [e1]

        evts = event_store.append_to_stream(stream_id1, [e3, e4])
        expect(evts).to eq [e3, e4]

        evts = event_store.append_to_stream(stream_id2, [e2])
        expect(evts).to eq [e2]

        evts = event_store.read_stream(stream_id1)
        expect(evts.map(&:id)).to eq [e1.id, e3.id, e4.id]

        evts = event_store.read_stream(stream_id2)
        expect(evts.map(&:id)).to eq [e2.id]
      end

      it 'reads stream ordered by seq, ascendant' do
        event_store.append_to_stream(stream_id1, [e3, e1, e4])
        evts = event_store.read_stream(stream_id1)
        expect(evts.map(&:id)).to eq [e1.id, e3.id, e4.id]
      end

      it 'blows up if passed unexpected sequence' do
        event_store.append_to_stream(stream_id1, [e1, e3, e4])
        e5 = UserDomain::NameChanged.new(stream_id: stream_id1, seq: 4, payload: { name: 'nope' })
        expect {
          event_store.append_to_stream(stream_id1, [e5], expected_seq: 2)
        }.to raise_error(Sourced::ConcurrencyError)
      end

      it 'blows up if events belong to different stream' do
        expect {
          event_store.append_to_stream(stream_id1, [e1, e2, e4])
        }.to raise_error(Sourced::DifferentStreamIdError)
      end

      it 'is a noop if empty events list' do
        evts = event_store.append_to_stream(stream_id1, [])
        expect(evts).to eq([])
      end
    end

    describe '#read_stream' do
      it 'supports :upto_seq argument' do
        event_store.append_to_stream(stream_id1, [e1, e3, e4])

        stream = event_store.read_stream(stream_id1, upto_seq: 2)
        expect(stream.map(&:id)).to eq [e1.id, e3.id]
      end
    end

    describe '#transaction' do
      it 'yields' do
        evts = []
        event_store.transaction do
          evts = event_store.append_to_stream(stream_id1, [e1, e3])
        end

        expect(evts).to eq [e1, e3]
      end
    end
  end
end
