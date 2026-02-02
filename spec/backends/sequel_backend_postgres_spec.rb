# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

RSpec.describe 'Sourced::Backends::SequelBackend with Postgres', type: :backend do
  subject(:backend) { Sourced::Backends::SequelBackend.new(db) }

  let(:db) do
    Sequel.postgres('sourced_test')
  end

  before do
    backend.setup!(Sourced.config)
    backend.uninstall if backend.installed?
    backend.install
  end

  it_behaves_like 'a backend'

  describe 'worker heartbeats and stale claim reaping' do
    it 'records heartbeats and releases stale claims' do
      # Prepare a consumer group and a stream with one event
      backend.register_consumer_group('group_hb')

      evt = BackendExamples::Tests::SomethingHappened1.parse(stream_id: 's-hb', seq: 1, payload: { account_id: 1 })
      backend.append_to_stream('s-hb', [evt])

      # First record a heartbeat for a worker that will become stale
      stale_time = Time.now - 3600
      backend.worker_heartbeat(['dead-worker-1'], at: stale_time)

      # Insert a stale claimed offset for the dead worker
      group_fk = db[:sourced_consumer_groups].where(group_id: 'group_hb').get(:id)
      stream_fk = db[:sourced_streams].where(stream_id: 's-hb').get(:id)

      off_id = db[:sourced_offsets].insert(
        group_id: group_fk,
        stream_id: stream_fk,
        global_seq: 0,
        created_at: stale_time,
        claimed: true,
        claimed_at: stale_time,
        claimed_by: 'dead-worker-1'
      )

      # Heartbeat two workers
      backend.worker_heartbeat(['live-worker-1', 'live-worker-2'])
      expect(db[:sourced_workers].where(id: 'live-worker-1').count).to eq(1)

      # Reap stale claims
      released = backend.release_stale_claims(ttl_seconds: 60)
      expect(released).to be >= 1

      row = db[:sourced_offsets].where(id: off_id).first
      expect(row[:claimed]).to eq(false)
      expect(row[:claimed_by]).to be_nil
      expect(row[:claimed_at]).to be_nil

      # Ensure live worker claims are not reaped
      # Create a fresh claimed offset for a live worker on a different stream
      evt2 = BackendExamples::Tests::SomethingHappened1.parse(stream_id: 's-hb-2', seq: 1, payload: { account_id: 2 })
      backend.append_to_stream('s-hb-2', [evt2])
      stream2_fk = db[:sourced_streams].where(stream_id: 's-hb-2').get(:id)

      fresh_time = Time.now
      off2_id = db[:sourced_offsets].insert(
        group_id: group_fk,
        stream_id: stream2_fk,
        global_seq: 0,
        created_at: fresh_time,
        claimed: true,
        claimed_at: fresh_time,
        claimed_by: 'live-worker-1'
      )

      backend.worker_heartbeat(['live-worker-1'])
      not_released = backend.release_stale_claims(ttl_seconds: 60)
      expect(not_released).to be_a(Integer)

      row2 = db[:sourced_offsets].where(id: off2_id).first
      expect(row2[:claimed]).to eq(true)
      expect(row2[:claimed_by]).to eq('live-worker-1')
    end
  end
end
