# frozen_string_literal: true

require 'spec_helper'
require 'sourced/committer_with_originator'

RSpec.describe Sourced::CommitterWithOriginator do
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
    it 'produces new array with originator first, and adds #originator_id to all events' do
      list = described_class.new(cmd, stage)
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
      expect(list.to_a.map(&:originator_id)).to eq [
        nil,
        cmd.id,
        cmd.id
      ]
    end

    context 'when no events' do
      let(:events) { [] }

      it 'produces an array with just the originator event' do

        list = described_class.new(cmd, stage)
        expect(list.to_a.map(&:class)).to eq [
          Sourced::UserDomain::UpdateUser
        ]
      end
    end
  end

  describe '#commit' do
    it 'commits stage and decorates events with originator' do
      list = described_class.new(cmd, stage)
      evts = list.commit do |seq, evts, entity|
        expect(seq).to eq 2
        expect(evts.map(&:id)).to eq [
          cmd.id,
          e1.id,
          e2.id
        ]
        expect(evts.map(&:originator_id)).to eq [
          nil,
          cmd.id,
          cmd.id
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
