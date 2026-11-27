# frozen_string_literal: true

# Sourced-specific extensions to the shared {Sourced::Message} class (provided by
# the `sourced-message` gem). These cover store-indexing concerns that depend on
# Sourced's own value objects, so they live here rather than in the gem.

require 'sourced/message'
require 'sourced/selector'

module Sourced
  # Optimistic concurrency: detects messages +selector+ selects appended after
  # +last_position+. Returned by {Store#read} and {Store#claim_next}; pass to
  # {Store#append} via +guard:+. {Selector::NONE} guards nothing.
  #
  # @!attribute selector [Selector] what {Store#read} read
  # @!attribute last_position [Integer]
  ConsistencyGuard = Data.define(:selector, :last_position)

  class Message
    # The class-level counterpart of {#to_message}: what handler declarations
    # (+evolve+, +reaction+, +command+) resolve to, and store and subscribe
    # handlers under. Anything standing for a message class answers it (ex. a
    # machine part, its +Part::State+).
    # @return [Class<Message>]
    def self.to_message_class = self

    # Key-value pairs from the payload's references ({.reference_attribute_names})
    # and {#tags}. Used by {Store#append} to index messages for querying.
    #
    # @return [Array<Array(String, String)>] pairs of [name, value], skipping nils
    # @raise [TagConflictError] if a tag names a payload attribute with a different
    #   value (compared as strings, as {#with_tags} does)
    def extracted_keys
      references = self.class.reference_attribute_names
      pairs = references.filter_map do |name|
        value = payload[name]
        [name.name, value.to_s] unless value.nil?
      end
      return pairs if tags.empty?

      declared = self.class.payload_attribute_names
      tag_pairs = tags.filter_map do |key, value|
        next [key.to_s, value] unless declared.include?(key)
        # A reference's pair is already in; a tag indexes an attribute that isn't one.
        next (references.include?(key) ? nil : [key.to_s, value]) if payload[key].to_s == value

        raise TagConflictError,
              "#{type} tag #{key}: #{value.inspect} conflicts with its payload value #{payload[key].inspect}"
      end
      pairs + tag_pairs
    end
  end
end
