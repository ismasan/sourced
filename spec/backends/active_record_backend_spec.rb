# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/active_record_backend'

RSpec.describe 'Sors::Backends::ActiveRecordBackend', type: :backend do
  subject(:backend) { Sors::Backends::ActiveRecordBackend.new }

  before :all do
    ActiveRecord::Base.establish_connection(
      adapter: 'postgresql',
      database: 'sors_test'
    )
  end

  before do
    backend.install unless backend.installed?
  end

  after do
    backend.clear!
  end

  it_behaves_like 'a backend'
end
