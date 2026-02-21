# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

module StaleClaimReaperTestMessages
  DeviceRegistered = Sourced::CCC::Message.define('reaper_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end
end

class ReaperTestProjector < Sourced::CCC::Projector
  partition_by :device_id
  consumer_group 'reaper-test-projector'

  state { |_| { devices: [] } }

  evolve StaleClaimReaperTestMessages::DeviceRegistered do |state, evt|
    state[:devices] << evt.payload.name
  end
end

RSpec.describe 'Store worker heartbeat and stale claim release' do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::CCC::Store.new(db) }

  before { store.install! }

  describe '#worker_heartbeat' do
    it 'inserts workers with last_seen' do
      count = store.worker_heartbeat(['w1', 'w2'])
      expect(count).to eq(2)

      rows = db[:ccc_workers].all
      expect(rows.size).to eq(2)
      expect(rows.map { |r| r[:id] }).to contain_exactly('w1', 'w2')
      rows.each { |r| expect(r[:last_seen]).not_to be_nil }
    end

    it 'updates existing workers last_seen' do
      early = Time.now - 300
      store.worker_heartbeat(['w1'], at: early)

      later = Time.now
      store.worker_heartbeat(['w1'], at: later)

      row = db[:ccc_workers].where(id: 'w1').first
      expect(row[:last_seen]).to eq(later.iso8601)
    end

    it 'deduplicates worker IDs' do
      count = store.worker_heartbeat(['w1', 'w1', 'w1'])
      expect(count).to eq(1)
      expect(db[:ccc_workers].count).to eq(1)
    end

    it 'returns 0 for empty array' do
      count = store.worker_heartbeat([])
      expect(count).to eq(0)
    end
  end

  describe '#release_stale_claims' do
    before do
      store.register_consumer_group('reaper-test-projector')

      # Append a message to create a partition
      store.append(
        StaleClaimReaperTestMessages::DeviceRegistered.new(payload: { device_id: 'd1', name: 'Sensor' })
      )

      # Claim the partition
      @claim = store.claim_next(
        'reaper-test-projector',
        partition_by: ['device_id'],
        handled_types: ['reaper_test.device.registered'],
        worker_id: 'w1'
      )
    end

    it 'releases claims from stale workers' do
      # Heartbeat far in the past
      store.worker_heartbeat(['w1'], at: Time.now - 300)

      released = store.release_stale_claims(ttl_seconds: 120)
      expect(released).to eq(1)

      # Verify the offset is unclaimed
      offset = db[:ccc_offsets].where(id: @claim.offset_id).first
      expect(offset[:claimed]).to eq(0)
      expect(offset[:claimed_at]).to be_nil
      expect(offset[:claimed_by]).to be_nil
    end

    it 'leaves claims from recently-heartbeated workers' do
      # Heartbeat just now
      store.worker_heartbeat(['w1'], at: Time.now)

      released = store.release_stale_claims(ttl_seconds: 120)
      expect(released).to eq(0)

      # Verify the offset is still claimed
      offset = db[:ccc_offsets].where(id: @claim.offset_id).first
      expect(offset[:claimed]).to eq(1)
      expect(offset[:claimed_by]).to eq('w1')
    end

    it 'returns 0 when no stale claims exist' do
      released = store.release_stale_claims(ttl_seconds: 120)
      # No heartbeat for w1, so no record in ccc_workers, so no stale workers found
      expect(released).to eq(0)
    end

    it 'only releases claims from stale workers, not healthy ones' do
      # Append another message for a different partition
      store.append(
        StaleClaimReaperTestMessages::DeviceRegistered.new(payload: { device_id: 'd2', name: 'Sensor 2' })
      )

      # Ack the first claim so d1 partition is free, then claim d2 with w2
      store.ack('reaper-test-projector', offset_id: @claim.offset_id, position: @claim.messages.last.position)

      claim2 = store.claim_next(
        'reaper-test-projector',
        partition_by: ['device_id'],
        handled_types: ['reaper_test.device.registered'],
        worker_id: 'w2'
      )

      # Re-claim d1 with w1
      claim1 = store.claim_next(
        'reaper-test-projector',
        partition_by: ['device_id'],
        handled_types: ['reaper_test.device.registered'],
        worker_id: 'w1'
      )

      # w1 is stale, w2 is healthy
      store.worker_heartbeat(['w1'], at: Time.now - 300)
      store.worker_heartbeat(['w2'], at: Time.now)

      released = store.release_stale_claims(ttl_seconds: 120)

      # Only w1's claims should be released
      if claim1
        expect(released).to eq(1)
        offset1 = db[:ccc_offsets].where(id: claim1.offset_id).first
        expect(offset1[:claimed]).to eq(0)
      end

      offset2 = db[:ccc_offsets].where(id: claim2.offset_id).first
      expect(offset2[:claimed]).to eq(1)
      expect(offset2[:claimed_by]).to eq('w2')
    end
  end
end

RSpec.describe Sourced::CCC::StaleClaimReaper do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::CCC::Store.new(db) }
  let(:logger) { instance_double('Logger', info: nil, warn: nil, debug: nil) }

  before { store.install! }

  describe '#heartbeat' do
    it 'calls worker_heartbeat with worker IDs from provider' do
      reaper = described_class.new(
        store: store,
        interval: 30,
        ttl_seconds: 120,
        worker_ids_provider: -> { ['w-0', 'w-1'] },
        logger: logger
      )

      reaper.send(:heartbeat)

      rows = db[:ccc_workers].all
      expect(rows.size).to eq(2)
      expect(rows.map { |r| r[:id] }).to contain_exactly('w-0', 'w-1')
    end
  end

  describe '#reap' do
    it 'calls release_stale_claims with configured TTL' do
      reaper = described_class.new(
        store: store,
        interval: 30,
        ttl_seconds: 60,
        logger: logger
      )

      expect(store).to receive(:release_stale_claims).with(ttl_seconds: 60).and_return(0)
      reaper.send(:reap)
    end
  end

  describe '#run and #stop' do
    it 'reaps on startup then stops' do
      reaper = described_class.new(
        store: store,
        interval: 0.01,
        ttl_seconds: 120,
        worker_ids_provider: -> { ['w-0'] },
        logger: logger
      )

      expect(store).to receive(:release_stale_claims).at_least(:once).and_return(0)

      thread = Thread.new { reaper.run }
      sleep 0.05
      reaper.stop
      thread.join(1)

      expect(logger).to have_received(:info).with('CCC::StaleClaimReaper: stopped')
    end
  end
end
