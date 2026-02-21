# frozen_string_literal: true

module Sourced
  module CCC
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
    # @param store [CCC::Store] the store to read from
    # @param partition_attrs [Hash{Symbol => String}] partition attribute values
    # @return [Array(reactor_instance, ReadResult)]
    #
    # @example
    #   decider, read_result = Sourced::CCC.load(MyDecider, store, course_id: 'Algebra', student_id: 'joe')
    #   decider.state       # evolved state
    #   read_result.guard   # ConsistencyGuard for subsequent appends
    def self.load(reactor_class, store, **partition_attrs)
      handled_types = reactor_class.handled_messages_for_evolve.map(&:type).uniq
      read_result = store.read_partition(partition_attrs, handled_types: handled_types)

      values = reactor_class.partition_keys.map { |k| partition_attrs[k]&.to_s }
      instance = reactor_class.new(values)
      instance.evolve(read_result.messages)

      [instance, read_result]
    end
  end
end

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
require 'sourced/ccc/dispatcher'
