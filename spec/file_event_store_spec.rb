# frozen_string_literal: true

require 'spec_helper'
require 'sourced/file_event_store'

RSpec.describe Sourced::FileEventStore do
  subject(:store) { described_class.new(dir: dir) }

  let(:dir) {
    dir = File.dirname(File.expand_path(__FILE__))
    File.join(dir, 'tmp')
  }

  after do
    FileUtils.rm_rf dir
  end

  it_behaves_like 'an event store'
end
