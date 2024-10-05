# frozen_string_literal: true

require 'sors'
require 'sors/backends/active_record_backend'

Sors.configure do |config|
  config.backend = Sors::Backends::ActiveRecordBackend.new
  config.logger = Rails.logger
end
