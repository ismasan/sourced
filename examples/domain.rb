# frozen_string_literal: true

require 'bundler'
Bundler.setup(:test)

require 'sors'
require 'sequel'
require_relative '../spec/support/test_domain'

# ActiveRecord::Base.establish_connection(adapter: 'postgresql', database: 'decider')
unless ENV['backend_configured']
  puts 'domain config'
  Sors.configure do |config|
    config.backend = Sequel.postgres('decider')
    # config.backend = Sors::Backends::ActiveRecordBackend.new
  end
  ENV['backend_configured'] = 'true'
end
