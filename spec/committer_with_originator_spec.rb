# frozen_string_literal: true

require 'spec_helper'
require 'sourced/committer_with_originator'

RSpec.describe Sourced::CommitterWithOriginator do
  let(:stage) { double('Committable', events: events, entity: {}) }
  let(:eid) { Sourced.uuid }
  let(:e1) { UserDomain::NameChanged.new(entity_id: eid, seq: 1, payload: { name: 'Ismael' }) }
  let(:e2) { UserDomain::AgeChanged.new(entity_id: eid, seq: 2, payload: { age: 42 }) }
  let(:events) { [e1, e2] }
  let(:cmd) do
    UserDomain::UpdateUser.new(entity_id: eid, payload: {
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
        UserDomain::UpdateUser,
        UserDomain::NameChanged,
        UserDomain::AgeChanged
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
          1,
          2,
          3
        ]
        expect(entity).to eq(stage.entity)
      end
      expect(stage).to have_received(:commit)
      expect(evts.size).to eq(3)
    end
  end
end
