# frozen_string_literal: true

require 'spec_helper'
require 'sors/backends/test_backend'

RSpec.describe Sors::Backends::TestBackend, type: :backend do
  subject(:backend) { described_class.new }

  it_behaves_like 'a backend'
end
