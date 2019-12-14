# frozen_string_literal: true

require 'spec_helper'
require 'sourced/entity_repo'

RSpec.describe Sourced::EntityRepo do
  let(:uuid) { Sourced.uuid }
  let(:event_store) { instance_double(Sourced::MemEventStore) }
  let(:past_events) { [instance_double(Sourced::Event)] }
  let(:session_events) { [instance_double(Sourced::Event)] }
  let(:session) { instance_double(Sourced::EntitySession, clear_events: session_events, last_persisted_seq: 3) }
  let(:session_builder) { double('EntitySession', load: session) }

  describe '#load' do
    it 'loads events from event store and invokes session.load' do
      repo = described_class.new(event_store: event_store)

      expect(event_store).to receive(:by_entity_id).with(uuid, from: 2).and_return past_events
      expect(session_builder).to receive(:load).with(uuid, past_events).and_return session
      expect(repo.load(uuid, session_builder, from: 2)).to eq(session)
    end
  end

  describe '#persist' do
    it 'takes events from EntitySession#clear_events and appends them to store' do
      repo = described_class.new(event_store: event_store)

      expect(event_store).to receive(:append).with(session_events, expected_seq: 3)
      expect(repo.persist(session)).to eq(session_events)
    end
  end

  describe '#persist_events' do
    it 'appends events to event store' do
      repo = described_class.new(event_store: event_store)

      expect(event_store).to receive(:append).with(session_events, expected_seq: nil)
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
