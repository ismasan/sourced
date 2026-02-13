# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'
require_relative 'support/unit_test_fixtures'
require_relative 'shared_examples/unit_examples'

RSpec.describe 'Sourced::Unit with SequelBackend (Postgres)' do
  let(:db) { Sequel.postgres('sourced_test') }
  let(:backend) do
    b = Sourced::Backends::SequelBackend.new(db)
    b.setup!(Sourced.config)
    b
  end

  before do
    backend.uninstall if backend.installed?
    backend.install
  end

  it_behaves_like 'a unit'
end
