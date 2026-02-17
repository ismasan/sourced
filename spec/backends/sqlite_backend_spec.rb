# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sqlite_backend'

RSpec.describe 'Sourced::Backends::SQLiteBackend', type: :backend do
  before(:all) do
    @db = Sequel.sqlite
    @backend = Sourced::Backends::SQLiteBackend.new(@db)
    @backend.install
  end

  subject(:backend) { @backend }
  let(:db) { @db }

  before do
    backend.clear!
  end

  it_behaves_like 'a backend'
end
