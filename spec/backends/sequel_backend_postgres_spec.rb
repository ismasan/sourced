# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

RSpec.describe 'Sourced::Backends::SequelBackend with Postgres', type: :backend do
  subject(:backend) { Sourced::Backends::SequelBackend.new(db) }

  let(:db) do
    Sequel.postgres('sourced_test')
  end

  before do
    if backend.installed?
      # Force drop and recreate tables to get latest schema
      %w[sourced_offsets sourced_commands sourced_events sourced_streams sourced_consumer_groups].each do |table|
        db.drop_table?(table.to_sym)
      end
    end
    backend.install
  end

  it_behaves_like 'a backend'
end
