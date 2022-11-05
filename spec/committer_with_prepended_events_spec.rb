# frozen_string_literal: true

require 'spec_helper'
require 'sourced/committer_with_prepended_events'

RSpec.describe Sourced::CommitterWithPrependedEvents do
  let(:stage) { instance_double(Sourced::Stage, events: events, last_committed_seq: 2, entity: {}) }
  let(:eid) { Sourced.uuid }
  let(:e1) { Sourced::UserDomain::NameChanged.new(stream_id: eid, seq: 3, payload: { name: 'Ismael' }) }
  let(:e2) { Sourced::UserDomain::AgeChanged.new(stream_id: eid, seq: 4, payload: { age: 42 }) }
  let(:events) { [e1, e2] }
  let(:cmd) do
    Sourced::UserDomain::UpdateUser.new(stream_id: eid, payload: {
      name: 'Ismael',
      age: 42
    })
  end

  before do
    allow(stage).to receive(:commit).and_yield(2, events, stage.entity)
  end

  describe '#to_a' do
    it 'produces new array with prepended events first, and shifts sequence numbers' do
      list = described_class.new(stage, cmd)
      expect(list.to_a.map(&:class)).to eq [
        Sourced::UserDomain::UpdateUser,
        Sourced::UserDomain::NameChanged,
        Sourced::UserDomain::AgeChanged
      ]
      expect(list.to_a.map(&:id)).to eq [
        cmd.id,
        e1.id,
        e2.id
      ]
      expect(list.to_a.map(&:seq)).to eq [3, 4, 5]
    end

    context 'when no events' do
      let(:events) { [] }

      it 'produces an array with just the prepended event' do

        list = described_class.new(stage, cmd)
        expect(list.to_a.map(&:class)).to eq [
          Sourced::UserDomain::UpdateUser
        ]
      end
    end
  end

  describe '#commit' do
    it 'commits stage and shifts sequence numbers' do
      list = described_class.new(stage, cmd)
      evts = list.commit do |seq, evts, entity|
        expect(seq).to eq 2
        expect(evts.map(&:id)).to eq [
          cmd.id,
          e1.id,
          e2.id
        ]
        expect(evts.map(&:seq)).to eq [
          3,
          4,
          5
        ]
        expect(entity).to eq(stage.entity)
      end
      expect(stage).to have_received(:commit)
      expect(evts.size).to eq(3)
    end
  end
end
