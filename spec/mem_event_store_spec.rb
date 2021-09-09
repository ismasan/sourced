# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::MemEventStore do
  it_behaves_like 'a Sourced event store' do
    subject(:event_store) { described_class.new }
  end
end
