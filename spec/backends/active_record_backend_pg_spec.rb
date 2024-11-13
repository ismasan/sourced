# frozen_string_literal: true

require 'spec_helper'
require 'sourced/backends/active_record_backend'

RSpec.describe Sourced::Backends::ActiveRecordBackend, skip: true, type: :backend do
  subject(:backend) { Sourced::Backends::ActiveRecordBackend.new }

  context 'with a PostgreSQL database' do
    it_behaves_like 'an ActiveRecord backend', adapter: 'postgresql', database: 'sors_test'
  end
end
