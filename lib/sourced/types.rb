# frozen_string_literal: true

require 'plumb'
require 'time'
require 'securerandom'

module Sourced
  module Types
    include Plumb::Types

    # A UUID string, or generate a new one
    AutoUUID = UUID::V4.default { SecureRandom.uuid }
  end
end
