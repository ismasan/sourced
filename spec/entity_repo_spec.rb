# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::EntityRepo do
  let(:uuid) { Sourced.uuid }
  let(:event_store) { Sourced::MemEventStore.new }
  let(:past_events) { [instance_double(Sourced::Event, entity_id: 'a')] }
  let(:stage_events) { [instance_double(Sourced::Event, entity_id: 'b')] }
  let(:stage) { instance_double(Sourced::Stage, entity: {}) }
  let(:stage_builder) { double('Stage', load: stage) }

  before do
    allow(stage).to receive(:commit).and_yield(3, stage_events, stage.entity)
  end

  describe '#load' do
    it 'loads events from event store and invokes stage.load' do
      repo = described_class.new(stage_builder, event_store: event_store)

      expect(event_store).to receive(:by_entity_id).with(uuid, after: 2).and_return past_events
      expect(stage_builder).to receive(:load).with(uuid, past_events).and_return stage
      expect(repo.load(uuid, after: 2)).to eq(stage)
    end
  end

  describe '#persist' do
    it 'takes events from Stage#commit and appends them to store' do
      repo = described_class.new(stage_builder, event_store: event_store)

      expect(event_store).to receive(:append).with(stage_events, expected_seq: 3).and_return(stage_events)
      expect(repo.persist(stage)).to eq(stage_events)
    end

    it 'yields entity and events within event store transaction' do
      the_entity = nil
      the_events = nil
      repo = described_class.new(stage_builder, event_store: event_store)
      repo.persist(stage) do |entity, events|
        the_entity = entity
        the_events = events
      end
      expect(the_entity).to eq(stage.entity)
      expect(the_events).to eq(stage_events)
    end
  end

  describe '#persist_with_originator' do
    it 'prepends event originator to event list, updates events :originator_id' do
    end
  end

  describe '#persist_events' do
    it 'appends events to event store' do
      repo = described_class.new(stage_builder, event_store: event_store)

      expect(event_store).to receive(:append).with(stage_events, expected_seq: nil).and_return(stage_events)
      expect(repo.persist_events(stage_events)).to eq(stage_events)
    end
  end

  describe '#build' do
    it 'builds pristine stage instance' do
      repo = described_class.new(stage_builder, event_store: event_store)

      expect(stage_builder).to receive(:load).with(uuid, []).and_return stage
      expect(repo.build(uuid)).to eq(stage)
    end
  end
end
