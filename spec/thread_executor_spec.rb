# frozen_string_literal: true

require 'spec_helper'
require 'sourced/thread_executor'

RSpec.describe Sourced::ThreadExecutor, type: :executor do
  subject(:executor) { described_class.new }

  it_behaves_like 'an executor'
end
