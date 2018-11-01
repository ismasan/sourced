require "sourced/version"
require 'securerandom'

module Sourced
  def self.uuid
    SecureRandom.uuid
  end
end

require 'sourced/eventable'
require 'sourced/event'
require 'sourced/handler'
require 'sourced/aggregate'
require 'sourced/aggregate_repo'
require 'sourced/mem_event_store'
