# frozen_string_literal: true

require 'plumb'
require 'time'
require 'securerandom'

module Sourced
  module Types
    include Plumb::Types

    # A UUID string, or generate a new one
    AutoUUID = UUID::V4.default { SecureRandom.uuid }

    # Deeply symbolize keys of a hash
    # Usage:
    #   SymbolizedHash.parse({ 'a' => { 'b' => 'c' } }) # => { a: { b: 'c' } }
    SymbolizedHash = Hash[
      # String keys are converted to symbols
      (Symbol | String.transform(::Symbol, &:to_sym)),
      # Hash values are recursively symbolized
      Any.defer { SymbolizedHash } | Any
    ]
  end
end
