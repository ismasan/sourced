# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::EntityRepo do
  let(:uuid) { Sourced.uuid }
  let(:event_store) { Sourced::MemEventStore.new }
  let(:past_events) { [instance_double(Sourced::Event, entity_id: 'a')] }
  let(:session_events) { [instance_double(Sourced::Event, entity_id: 'b')] }
  let(:session) { instance_double(Sourced::EntitySession, entity: {}) }
  let(:session_builder) { double('EntitySession', load: session) }

  before do
    allow(session).to receive(:commit).and_yield(3, session_events)
  end

  describe '#load' do
    it 'loads events from event store and invokes session.load' do
      repo = described_class.new(event_store: event_store)

      expect(event_store).to receive(:by_entity_id).with(uuid, after: 2).and_return past_events
      expect(session_builder).to receive(:load).with(uuid, past_events).and_return session
      expect(repo.load(uuid, session_builder, after: 2)).to eq(session)
    end
  end

  describe '#persist' do
    it 'takes events from EntitySession#commit and appends them to store' do
      repo = described_class.new(event_store: event_store)

      expect(event_store).to receive(:append).with(session_events, expected_seq: 3).and_return(session_events)
      expect(repo.persist(session)).to eq(session_events)
    end

    context 'with subscribed sync reactors' do
      it 'calls reactors with latest entity and events, inside a transaction' do
        sub1 = double('Reactor1', call: true)
        sub2 = double('Reactor2', call: true)

        repo = described_class.new(event_store: event_store, subscribers: [sub1, sub2])
        repo.persist(session)

        expect(sub1).to have_received(:call).with(session_events, session.entity)
        expect(sub2).to have_received(:call).with(session_events, session.entity)
      end
    end
  end

  describe '#persist_events' do
    it 'appends events to event store' do
      repo = described_class.new(event_store: event_store)

      expect(event_store).to receive(:append).with(session_events, expected_seq: nil).and_return(session_events)
      expect(repo.persist_events(session_events)).to eq(session_events)
    end
  end

  describe '#build' do
    it 'builds pristine session instance' do
      repo = described_class.new(event_store: event_store)

      expect(session_builder).to receive(:load).with(uuid, []).and_return session
      expect(repo.build(uuid, session_builder)).to eq(session)
    end
  end
end
