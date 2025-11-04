# frozen_string_literal: true

require 'plumb'
require 'time'
require 'securerandom'

module Sourced
  # Type definitions and validations for Sourced using the Plumb gem.
  # This module provides custom types for UUID generation, hash symbolization,
  # and interface validation used throughout the Sourced framework.
  #
  # @example Using AutoUUID type
  #   AutoUUID.parse(nil)  # => generates new UUID
  #   AutoUUID.parse("existing-uuid")  # => "existing-uuid"
  #
  # @example Using SymbolizedHash type
  #   SymbolizedHash.parse({ 'a' => { 'b' => 'c' } })  # => { a: { b: 'c' } }
  #
  # @see https://github.com/ismasan/plumb Plumb gem documentation
  module Types
    include Plumb::Types

    # A type that accepts UUID strings or generates a new UUID if none provided.
    # Useful for default values in message definitions where a UUID is required.
    #
    # @example Generate new UUID when nil
    #   AutoUUID.parse(nil)  # => "550e8400-e29b-41d4-a716-446655440000"
    # @example Use existing UUID
    #   AutoUUID.parse("test-uuid")  # => "test-uuid"
    AutoUUID = UUID::V4.default { SecureRandom.uuid }
  end
end
