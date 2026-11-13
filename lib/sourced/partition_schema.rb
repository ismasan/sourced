# frozen_string_literal: true

module Sourced
  # Checks a reactor's handled messages against its +partition_schema+
  # (see {Consumer#partition_by}). Built-in reactors run it from +validate!+,
  # which the {Router} calls, so hand-defined messages are held to the same
  # keys and types as those built with {Consumer#Command} / {Consumer#Event}.
  #
  # The store reads a message into a partition when every partition key it
  # declares matches, so messages carrying some of the keys are legitimate.
  # The rules, per role:
  #
  # * Commands, reactions and a machine's +on+ events pick out one partition:
  #   they must declare every key, as required attributes. So must a Decider's
  #   declared outputs, or they'd be read into every partition sharing a key.
  # * Any other handled message must declare at least one key, or no
  #   partition ever reads it.
  # * Every key a message declares must be the schema's type, or stricter
  #   (Plumb subtyping: +String.present+ passes for +String+, +Integer+ doesn't).
  # * At least one handled message declares every key, or no partition is
  #   ever created.
  # * The claim selector's keys are the partition keys.
  # * The history selector's keys are the claim selector's, so a claim can
  #   bind them ({.check_history_keys!}).
  #
  # Id-partitioned reactors (no schema) are only checked for the last rule.
  module PartitionSchema
    extend self

    FULL_ROLES = {
      handled_commands: 'command', handled_events: 'on event', handled_messages_for_react: 'reaction',
      produced_messages: 'output'
    }.freeze

    # @param reactor [#partition_schema, #claim_selector, #history_selector] +partition_schema+
    #   is a +Plumb::HashClass+
    # @param error [Class] the exception class to raise
    # @raise [DefinitionError]
    # @return [void]
    def check!(reactor, error: DefinitionError)
      return unless reactor.respond_to?(:partition_schema)

      schema = reactor.partition_schema.to_h.transform_keys(&:to_sym)
      return check_history_keys!(reactor, error:) if schema.empty? || reactor.claim_selector.id_partitioned?

      full = FULL_ROLES.each_with_object({}) do |(method, role), roles|
        next unless reactor.respond_to?(method)

        reactor.public_send(method).each { |klass| roles[klass] ||= role }
      end
      selector = reactor.history_selector
      # The claim selector first: a history selector is held to its keys.
      check_claim_keys!(reactor, schema, error)
      check_history_keys!(reactor, error:)
      messages = (full.keys + reactor.claim_selector.types + selector.types).uniq
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

    # @param reactor [#claim_selector, #history_selector]
    # @param error [Class] the exception class to raise
    # @raise [DefinitionError]
    # @return [void]
    def check_history_keys!(reactor, error: DefinitionError)
      claim_keys = reactor.claim_selector.keys
      unbound = reactor.history_selector.keys - claim_keys
      return if unbound.empty?

      raise error, "#{reactor}: its history_selector is scoped by #{unbound.join(', ')}, which its " \
                   "claim_selector (#{claim_keys.join(', ')}) doesn't partition by, so a claim can't bind them"
    end

    private def check_claim_keys!(reactor, schema, error)
      claim_selector = reactor.claim_selector
      return if claim_selector.none? || claim_selector.keys.sort == schema.keys.sort

      raise error, "#{reactor}: its claim_selector is scoped by #{claim_selector.keys.join(', ')}, " \
                   "not its partition keys (#{schema.keys.join(', ')})"
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
