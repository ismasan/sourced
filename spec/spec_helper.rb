# frozen_string_literal: true

require 'dotenv/load'
require 'sourced'
require 'debug'
require 'logger'
require 'timecop'
require_relative './shared_examples/backend_examples'
require_relative './shared_examples/executor_examples'

ENV['ENVIRONMENT'] ||= 'test'

Sourced.configure do |config|
  if ENV['LOGS_DIR']
    FileUtils.mkdir_p(ENV['LOGS_DIR'])
    config.logger = Logger.new(File.join(ENV['LOGS_DIR'], 'test.log'))
  else
    config.logger = Logger.new(STDOUT)
  end
end

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.include BackendExamples, type: :backend
  config.include ExecutorExamples, type: :executor
end
