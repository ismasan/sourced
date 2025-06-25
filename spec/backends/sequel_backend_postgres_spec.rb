# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

RSpec.describe 'Sourced::Backends::SequelBackend with Postgres', type: :backend do
  subject(:backend) { Sourced::Backends::SequelBackend.new(db) }

  let(:db) do
    Sequel.postgres('sourced_test')
  end

  before do
    backend.uninstall
    backend.install
  end

  it_behaves_like 'a backend'
end
