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

    # Turn "Foo::Bar::FooBar" into "foo.bar.foo_bar"
    ModulesToDots = String.transform(::String) { |v| v.gsub('::', '.') }
    Underscore = String.build(::String) { |v|
      v
        .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
        .gsub(/([a-z\d])([A-Z])/, '\1_\2')
        .gsub(/-/, '_')
        .downcase
    }
    ModuleToMessageType = ModulesToDots >> Underscore

    # An arbitrary JSON-native value: scalars, and arrays/hashes of them, all
    # the way down. Use it for message attributes whose shape isn't known up
    # front but which still have to survive a round trip through the store's
    # JSON columns — {Sourced::DurableWorkflow}'s workflow context and step
    # outputs are the built-in example.
    #
    # Values a JSON document can't carry, such as a Date or a Symbol, are
    # rejected at append time. For values with a known type, declare the type
    # and the store's codec encodes and decodes it.
    JSONData = String | Integer | Float | True | False | Nil |
               Array[Any.defer { JSONData }] |
               Hash[Symbol | String, Any.defer { JSONData }]
  end
end
