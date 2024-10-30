# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/active_record_backend'

RSpec.describe Sors::Backends::ActiveRecordBackend, skip: true, type: :backend do
  subject(:backend) { Sors::Backends::ActiveRecordBackend.new }

  context 'with a PostgreSQL database' do
    it_behaves_like 'an ActiveRecord backend', adapter: 'postgresql', database: 'sors_test'
  end
end
