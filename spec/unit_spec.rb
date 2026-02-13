# frozen_string_literal: true

require 'spec_helper'
require_relative 'support/unit_test_fixtures'
require_relative 'shared_examples/unit_examples'

RSpec.describe Sourced::Unit do
  describe 'with TestBackend' do
    let(:backend) { Sourced::Backends::TestBackend.new }

    it_behaves_like 'a unit'
  end
end
