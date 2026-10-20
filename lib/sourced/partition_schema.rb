# frozen_string_literal: true

module Sourced
  # Checks a reactor's handled messages against its +partition_schema+
  # (see {Consumer#partition_by}). The {Router} runs it for every reactor that
  # declares one, so hand-defined messages are held to the same key types
  # as those built with {Consumer#Command} / {Consumer#Event}.
  #
  # A message carries a partition key in its payload or as a tag
  # ({Message#with_tags}), so declaring keys is optional, and a missing key is
  # caught at runtime ({Sourced.handle!} raises {MissingPartitionKeyError}).
  # The one rule: every key a message declares must be the schema's type, or
  # stricter (Plumb subtyping: +String.present+ passes for +String+, +Integer+ doesn't).
  #
  # Id-partitioned reactors (no schema) aren't checked.
  module PartitionSchema
    extend self

    ROLES = { handled_commands: 'command', handled_events: 'on event', handled_messages_for_react: 'reaction' }.freeze

    # @param reactor [#partition_schema, #handled_messages] +partition_schema+ is a +Plumb::HashClass+
    # @param error [Class] the exception class to raise
    # @raise [DefinitionError]
    # @return [void]
    def check!(reactor, error: DefinitionError)
      return unless reactor.respond_to?(:partition_schema)

      schema = reactor.partition_schema.to_h.transform_keys(&:to_sym)
      return if schema.empty? || IdPartition.keys?(schema.keys)

      roles = ROLES.each_with_object({}) do |(method, role), acc|
        next unless reactor.respond_to?(method)

        reactor.public_send(method).each { |klass| acc[klass] ||= role }
      end
      evolved = reactor.respond_to?(:handled_messages_for_evolve) ? reactor.handled_messages_for_evolve : Plumb::BLANK_ARRAY
      (roles.keys + reactor.handled_messages + evolved).uniq.each do |klass|
        check_types!(reactor, schema, klass, roles.fetch(klass, 'message'), attributes_of(klass), error)
      end
    end

    # @return [Hash{Symbol => Array(Plumb::Composable, Boolean)}] each payload
    #   attribute's type, and whether it's optional
    def attributes_of(klass)
      return {} unless klass.respond_to?(:payload_attribute_names) && klass.payload_attribute_names.any?

      klass::Payload._schema.to_h.to_h { |key, type| [key.to_sym, [type, key.optional?]] }
    end

    private def check_types!(reactor, schema, klass, role, attributes, error)
      schema.each do |key, expected|
        next unless (type, = attributes[key])
        next if type <= expected

        raise error, "#{reactor}: #{role} #{klass} declares #{key} as #{type.inspect}, " \
                     "but #{reactor} partitions by #{key} as #{expected.inspect}"
      end
    end
  end
end
