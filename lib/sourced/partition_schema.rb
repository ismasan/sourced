# frozen_string_literal: true

module Sourced
  # Checks a reactor's handled messages against its +partition_schema+
  # (see {Consumer#partition_by}). The {Router} runs it for every reactor that
  # declares one, so hand-defined messages are held to the same keys and types
  # as those built with {Consumer#Command} / {Consumer#Event}.
  #
  # The store reads a message into a partition when every partition key it
  # declares matches, so messages carrying some of the keys are legitimate.
  # The rules, per role:
  #
  # * Commands, reactions and a machine's +on+ events pick out one partition:
  #   they must declare every key, as required attributes.
  # * Any other handled message must declare at least one key, or no
  #   partition ever reads it.
  # * Every key a message declares must be the schema's type, or stricter
  #   (Plumb subtyping: +String.present+ passes for +String+, +Integer+ doesn't).
  # * At least one handled message declares every key, or no partition is
  #   ever created.
  #
  # Id-partitioned reactors (no schema) aren't checked.
  module PartitionSchema
    extend self

    FULL_ROLES = { handled_commands: 'command', handled_events: 'on event', handled_messages_for_react: 'reaction' }.freeze

    # @param reactor [#partition_schema, #handled_messages] +partition_schema+ is a +Plumb::HashClass+
    # @param error [Class] the exception class to raise
    # @raise [DefinitionError]
    # @return [void]
    def check!(reactor, error: DefinitionError)
      return unless reactor.respond_to?(:partition_schema)

      schema = reactor.partition_schema.to_h.transform_keys(&:to_sym)
      return if schema.empty? || IdPartition.keys?(schema.keys)

      full = FULL_ROLES.each_with_object({}) do |(method, role), roles|
        next unless reactor.respond_to?(method)

        reactor.public_send(method).each { |klass| roles[klass] ||= role }
      end
      evolved = reactor.respond_to?(:handled_messages_for_evolve) ? reactor.handled_messages_for_evolve : []
      messages = (full.keys + reactor.handled_messages + evolved).uniq
      return if messages.empty?

      complete = false
      messages.each do |klass|
        attributes = attributes_of(klass)
        role = full.fetch(klass, 'message')
        check_types!(reactor, schema, klass, role, attributes, error)
        declared = schema.keys.select { |key| attributes.key?(key) && !attributes[key].last }
        complete ||= declared.size == schema.size

        if full.key?(klass)
          missing = schema.keys - declared
          raise error, "#{reactor}: #{role} #{klass} must declare #{missing.join(', ')}" if missing.any?
        elsif (schema.keys & attributes.keys).empty?
          raise error, "#{reactor}: #{role} #{klass} declares none of the partition keys " \
                       "#{schema.keys.join(', ')}, so no partition reads it"
        end
      end
      return if complete

      raise error, "#{reactor}: no handled message declares every partition key " \
                   "#{schema.keys.join(', ')}, so no partition is ever created"
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
