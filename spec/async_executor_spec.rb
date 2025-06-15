# frozen_string_literal: true

require 'spec_helper'
require 'sourced/async_executor'

RSpec.describe Sourced::AsyncExecutor, type: :executor do
  subject(:executor) { described_class.new }

  it_behaves_like 'an executor'
end
