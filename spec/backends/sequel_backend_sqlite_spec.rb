# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/sequel_backend'

RSpec.describe 'Sors::Backends::SequelBackend with sqlite', type: :backend do
  subject(:backend) { Sors::Backends::SequelBackend.new(db) }

  let(:db) do
    Sequel.sqlite
  end

  before do
    backend.install unless backend.installed?
  end

  it_behaves_like 'a backend'
end
