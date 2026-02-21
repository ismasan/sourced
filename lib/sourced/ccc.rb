# frozen_string_literal: true

module Sourced
  module CCC
    # @return [Configuration] the global CCC configuration instance
    def self.config
      @config ||= Configuration.new
    end

    # Configure the CCC module. Calls setup! after yielding.
    # @yieldparam config [Configuration]
    def self.configure(&)
      yield config if block_given?
      config.setup!
      config.freeze
    end

    # Register a reactor class with the global router.
    # Triggers setup! if not already done.
    # @param reactor [Class] a CCC reactor class
    def self.register(reactor)
      config.setup!
      config.router.register(reactor)
    end

    # @return [CCC::Store] the global store (triggers setup! if needed)
    def self.store
      config.setup!
      config.store
    end

    # @return [CCC::Router] the global router (triggers setup! if needed)
    def self.router
      config.setup!
      config.router
    end

    # Reset the global configuration. For test teardown.
    def self.reset!
      @config = nil
    end

    # Load a reactor instance from its event history using AND-filtered partition reads.
    # Returns the evolved instance and a ReadResult (with .messages and .guard).
    #
    # Uses {Store#read_partition} which filters at the SQL level: a message is
    # included only when every partition attribute it declares matches the given
    # value. Messages that don't declare a partition attribute pass through
    # (e.g. CourseCreated with only +course_id+ is included even when
    # +student_id+ is in the partition).
    #
    # @param reactor_class [Class] a CCC reactor class (Decider, Projector, or any class
    #   extending CCC::Consumer that includes CCC::Evolve)
    # @param store [CCC::Store, nil] the store to read from (defaults to CCC.store)
    # @param partition_attrs [Hash{Symbol => String}] partition attribute values
    # @return [Array(reactor_instance, ReadResult)]
    #
    # @example
    #   decider, read_result = Sourced::CCC.load(MyDecider, course_id: 'Algebra', student_id: 'joe')
    #   decider.state       # evolved state
    #   read_result.guard   # ConsistencyGuard for subsequent appends
    def self.load(reactor_class, store: nil, **partition_attrs)
      store ||= self.store
      handled_types = reactor_class.handled_messages_for_evolve.map(&:type).uniq
      read_result = store.read_partition(partition_attrs, handled_types: handled_types)

      values = reactor_class.partition_keys.map { |k| partition_attrs[k]&.to_s }
      instance = reactor_class.new(values)
      instance.evolve(read_result.messages)

      [instance, read_result]
    end
  end
end

require 'sourced/ccc/configuration'
require 'sourced/ccc/message'
require 'sourced/ccc/store'
require 'sourced/ccc/actions'
require 'sourced/ccc/consumer'
require 'sourced/ccc/evolve'
require 'sourced/ccc/react'
require 'sourced/ccc/sync'
require 'sourced/ccc/decider'
require 'sourced/ccc/projector'
require 'sourced/ccc/router'
require 'sourced/ccc/worker'
require 'sourced/ccc/stale_claim_reaper'
require 'sourced/ccc/dispatcher'
require 'sourced/ccc/supervisor'
