require "sourced/version"
require 'securerandom'

module Sourced
  def self.uuid
    SecureRandom.uuid
  end
end
