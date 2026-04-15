# frozen_string_literal: true

require 'sourced'
require 'logger'
require 'timecop'
require 'sourced/testing/rspec'

ENV['ENVIRONMENT'] ||= 'test'

RSpec.configure do |config|
  config.example_status_persistence_file_path = '.rspec_status'
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.include Sourced::Testing::RSpec
end
