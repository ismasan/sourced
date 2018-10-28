require "sourced/version"
require 'securerandom'

module Sourced
  def self.uuid
    SecureRandom.uuid
  end
end

require 'sourced/eventable'
require 'sourced/event'
