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

  # Returned by {Store#read} for conditions, for optimistic concurrency.
  # Pass to {Store#append} via +guard:+ to detect conflicting writes.
  ConsistencyGuard = Data.define(:conditions, :last_position)

  # Partitioning by {Message#id}: one partition per message, the store's queue
  # case. The id is indexed under a reserved key name, chosen not to collide
  # with a payload attribute.
  module IdPartition
    extend self

    KEY = '__id'

    # Whether +keys+ (a +partition_by+, or a claimed partition's keys) are the id partition.
    #
    # @param keys [Array<String, Symbol>]
    # @return [Boolean]
    def keys?(keys) = keys.size == 1 && keys.first.to_s == KEY
  end

  # The messages of +types+ in a partition, matched per message the way claims
  # match them: a message is in the partition when every partition key it
  # carries (in its payload or its tags) has the partition's value. Messages
  # carrying only some of the keys (ex. foreign events) are read into every
  # partition sharing those values. Pass to {Store#read}.
  #
  # Key presence is per message, not per class (a tag may carry any key), so
  # this can't be expressed as {QueryCondition}s.
  #
  # @!attribute attrs [Hash{String => String}] partition values
  # @!attribute types [Array<String>] message type strings
  PartitionQuery = Data.define(:attrs, :types) do
    def initialize(attrs:, types:)
      super(
        attrs: attrs.to_h { |k, v| [k.to_s, v.to_s] }.freeze,
        types: types.map(&:to_s).uniq.freeze
      )
    end
  end

  # Returned by {Store#read} for a {PartitionQuery}, by {Store#read_partition}
  # and by {Store#claim_next}. {Store#append} raises when a message in the
  # partition (as {PartitionQuery} defines it) was appended after +last_position+.
  PartitionGuard = Data.define(:attrs, :types, :last_position) do
    def query = PartitionQuery.new(attrs:, types:)
  end

  class Message
    # Build a {QueryCondition} for the intersection of this message's declared
    # attributes and the given key-value pairs. Attributes not declared on this
    # message class are silently ignored. Returns an array with a single condition
    # containing all matching attrs, or an empty array if none match.
    #
    # Conditions only see declared attributes, not {#tags}: use a
    # {PartitionQuery} to read a partition.
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

    # Key-value pairs from all top-level payload attributes and {#tags}.
    # Used by {Store#append} to index messages for querying.
    #
    # @return [Array<Array(String, String)>] pairs of [name, value], skipping nils
    # @raise [TagConflictError] if a tag names a payload attribute with a different
    #   value (compared as strings, as {#with_tags} does)
    def extracted_keys
      pairs = payload ? payload.to_h.filter_map { |k, v| [k.to_s, v.to_s] unless v.nil? } : Plumb::BLANK_ARRAY
      return pairs if tags.empty?

      declared = self.class.payload_attribute_names
      tag_pairs = tags.filter_map do |key, value|
        next [key.to_s, value] unless declared.include?(key)
        next if payload[key].to_s == value

        raise TagConflictError,
              "#{type} tag #{key}: #{value.inspect} conflicts with its payload value #{payload[key].inspect}"
      end
      pairs + tag_pairs
    end
  end
end
