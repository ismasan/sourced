# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/sequel_backend'

RSpec.describe 'Sourced::Backends::SequelBackend with sqlite', skip: true, type: :backend do
  subject(:backend) { Sourced::Backends::SequelBackend.new(db) }

  let(:db) do
    Sequel.sqlite
  end

  before do
    backend.install unless backend.installed?
  end

  it_behaves_like 'a backend'
end
