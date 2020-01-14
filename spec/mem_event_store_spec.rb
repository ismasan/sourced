# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::MemEventStore do
  subject(:store) { described_class.new }

  it_behaves_like 'an event store'
end
