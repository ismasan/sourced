# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Worker do
  let(:router) { instance_double(Sourced::Router) }
  let(:logger) { instance_double('Logger', warn: nil, info: nil) }
  let(:reactor1) { double('Reactor1', handled_messages: ['event1']) }
  let(:reactor2) { double('Reactor2', handled_messages: ['event2']) }
  let(:reactor3) { double('Reactor3', handled_messages: []) }
  let(:reactors) { [reactor1, reactor2, reactor3] }

  before do
    allow(router).to receive(:async_reactors).and_return(reactors)
    allow(router).to receive(:handle_next_event_for_reactor).and_return(true)
  end

  describe '#tick' do
    subject(:worker) do
      described_class.new(
        logger:,
        name: 'test-worker',
        shuffler: ->(arr) { arr }, # No shuffle for predictability
        router:
      )
    end

    it 'delegates to Router#handle_next_event_for_reactor with current reactor and worker name' do
      expect(router).to receive(:handle_next_event_for_reactor)
        .with(reactor1, worker.name, batch_size: 1)
        .and_return(true)

      result = worker.tick

      expect(result).to eq(true)
    end

    it 'cycles through reactors in round-robin fashion when called repeatedly' do
      # First call should use reactor1 (index 0)
      expect(router).to receive(:handle_next_event_for_reactor)
        .with(reactor1, worker.name, batch_size: 1)
        .and_return(true)
      worker.tick

      # Second call should use reactor2 (index 1)  
      expect(router).to receive(:handle_next_event_for_reactor)
        .with(reactor2, worker.name, batch_size: 1)
        .and_return(false)
      worker.tick

      # Fourth call should wrap around to reactor1 (index 0)
      expect(router).to receive(:handle_next_event_for_reactor)
        .with(reactor1, worker.name, batch_size: 1)
        .and_return(false)
      worker.tick
    end

    it 'returns the result from Router#handle_next_event_for_reactor' do
      allow(router).to receive(:handle_next_event_for_reactor).and_return(false)
      expect(worker.tick).to eq(false)

      allow(router).to receive(:handle_next_event_for_reactor).and_return(true)
      expect(worker.tick).to eq(true)
    end

    context 'when called with a specific reactor' do
      it 'uses the provided reactor instead of cycling' do
        expect(router).to receive(:handle_next_event_for_reactor)
          .with(reactor2, worker.name, batch_size: 1)
          .and_return(true)

        worker.tick(reactor2)
      end

      it 'does not advance the reactor index when using a specific reactor' do
        # Call with specific reactor
        worker.tick(reactor2)
        
        # Next call without reactor should still use reactor1 (first in cycle)
        expect(router).to receive(:handle_next_event_for_reactor)
          .with(reactor1, worker.name, batch_size: 1)
          .and_return(true)
        worker.tick
      end
    end
  end
end
