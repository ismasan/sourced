# frozen_string_literal: true

# Sourced-specific extensions to the shared {Sourced::Message} class (provided by
# the `sourced-message` gem). These cover store-indexing concerns that depend on
# Sourced's own value objects, so they live here rather than in the gem.

require 'sourced/message'

module Sourced
  # A query condition for reading messages from the store.
  # Matches on (message_type AND all attrs key-value pairs).
  # Multiple conditions are OR'd when passed to {Store#read}.
  QueryCondition = Data.define(:message_type, :attrs)

  # Returned by {Store#read} and {Store#claim_next} for optimistic concurrency.
  # Pass to {Store#append} via +guard:+ to detect conflicting writes.
  ConsistencyGuard = Data.define(:conditions, :last_position)

  class Message
    # Build a {QueryCondition} for the intersection of this message's declared
    # attributes and the given key-value pairs. Attributes not declared on this
    # message class are silently ignored. Returns an array with a single condition
    # containing all matching attrs, or an empty array if none match.
    #
    # @param attrs [Hash{Symbol => String}] partition attribute values
    # @return [Array<QueryCondition>]
    #
    # @example
    #   CourseCreated.to_conditions(course_name: 'Algebra', user_id: 'joe')
    #   # => [QueryCondition('course.created', { course_name: 'Algebra' })]
    #   # user_id ignored — CourseCreated doesn't declare it
    def self.to_conditions(**attrs)
      supported = payload_attribute_names
      matched = attrs.select { |key, _| supported.include?(key) }
                     .transform_values(&:to_s)
      return [] if matched.empty?

      [QueryCondition.new(message_type: type, attrs: matched)]
    end

    # Auto-extract key-value pairs from all top-level payload attributes.
    # Used by {Store#append} to index messages for querying.
    #
    # @return [Array<Array(String, String)>] pairs of [name, value], skipping nils
    def extracted_keys
      return [] unless payload

      payload.to_h.filter_map { |k, v|
        [k.to_s, v.to_s] unless v.nil?
      }
    end
  end
end
