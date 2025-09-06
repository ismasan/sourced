# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/test_backend'

RSpec.describe Sourced::Backends::TestBackend, type: :backend do
  subject(:backend) { described_class.new }

  it_behaves_like 'a backend'

  describe 'housekeeping interfaces' do
    it 'exposes worker_heartbeat and release_stale_claims' do
      expect(backend.worker_heartbeat([])).to eq(0)
      expect(backend.worker_heartbeat(['w1', 'w2'])).to eq(2)
      expect(backend.release_stale_claims(ttl_seconds: 10)).to eq(0)
    end
  end
end
