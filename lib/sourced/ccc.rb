# frozen_string_literal: true

require 'sourced/injector'

module Sourced
  module CCC
    # @return [Configuration] the global CCC configuration instance
    def self.config
      @config ||= Configuration.new
    end

    # Configure the CCC module. Stores the block for re-running after fork
    # (see {.setup!}), then runs it immediately.
    # @yieldparam config [Configuration]
    def self.configure(&block)
      @configure_block = block
      setup!
    end

    # Run (or re-run) the configure block on a fresh Configuration.
    # Safe to call after a process fork to re-establish database connections.
    # @return [void]
    def self.setup!
      @config = Configuration.new
      @configure_block&.call(@config)
      @config.setup!
      @config.freeze
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

    # Stop a consumer group and invoke the reactor's +on_stop+ callback.
    # Delegates to {Router#stop_consumer_group}.
    #
    # @param reactor_or_id [Class, String] a registered reactor class, or its +group_id+ string
    # @param message [String, nil] optional reason for stopping
    # @return [void]
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    # @see Router#stop_consumer_group
    def self.stop_consumer_group(reactor_or_id, message = nil)
      config.router.stop_consumer_group(reactor_or_id, message)
    end

    # Reset a consumer group and invoke the reactor's +on_reset+ callback.
    # Delegates to {Router#reset_consumer_group}.
    #
    # @param reactor_or_id [Class, String] a registered reactor class, or its +group_id+ string
    # @return [void]
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    # @see Router#reset_consumer_group
    def self.reset_consumer_group(reactor_or_id)
      config.router.reset_consumer_group(reactor_or_id)
    end

    # Start a consumer group and invoke the reactor's +on_start+ callback.
    # Delegates to {Router#start_consumer_group}.
    #
    # @param reactor_or_id [Class, String] a registered reactor class, or its +group_id+ string
    # @return [void]
    # @raise [ArgumentError] if +reactor_or_id+ is a String that doesn't match any registered reactor
    # @see Router#start_consumer_group
    def self.start_consumer_group(reactor_or_id)
      config.router.start_consumer_group(reactor_or_id)
    end

    # Reset the global configuration. For test teardown.
    def self.reset!
      @config = nil
      @configure_block = nil
      @topology = nil
    end

    # Build and cache the topology graph from all reactors registered with
    # the global {.router}. The result is memoized; call {.reset_topology}
    # to force a rebuild after registering new reactors.
    #
    # @return [Array<Sourced::Topology::CommandNode, Sourced::Topology::EventNode,
    #   Sourced::Topology::AutomationNode, Sourced::Topology::ReadModelNode>]
    #   flat array of topology node structs
    #
    # @example Inspect the global topology
    #   Sourced::CCC.register(MyDecider)
    #   Sourced::CCC.register(MyProjector)
    #
    #   Sourced::CCC.topology.each do |node|
    #     puts "#{node.type}: #{node.name} (#{node.id})"
    #   end
    #
    # @see CCC::Topology.build
    def self.topology
      @topology ||= CCC::Topology.build(router.reactors)
    end

    # Clear the cached topology so it is rebuilt on next access to {.topology}.
    # Useful after registering additional reactors at runtime.
    #
    # @return [nil]
    #
    # @example
    #   Sourced::CCC.register(LateAddedDecider)
    #   Sourced::CCC.reset_topology
    #   Sourced::CCC.topology # now includes LateAddedDecider
    def self.reset_topology
      @topology = nil
    end

    # Returned by {.handle!} with command, reactor instance, and new events.
    # Supports array destructuring: +cmd, reactor, events = CCC.handle!(cmd, MyDecider)+
    HandleResult = Data.define(:command, :reactor, :events) do
      def to_ary = [command, reactor, events]
    end

    # Handle a command synchronously: validate, load history, decide, append, and ACK.
    #
    # 1. Validates the command via +command.valid?+
    # 2. If invalid, returns immediately with the command, an uninitialized reactor, and empty events
    # 3. Loads the reactor's history from the command's partition attributes
    # 4. Evolves the reactor from history and runs the decider
    # 5. Appends the command and correlated events to the store with optimistic concurrency
    # 6. Advances consumer group offsets for registered reactors so background workers skip
    #    the already-handled command
    #
    # @param reactor_class [Class] a CCC::Decider (or any reactor extending Consumer + Evolve)
    # @param command [CCC::Command] the command to handle (must respond to +valid?+)
    # @param store [CCC::Store, nil] the store to use (defaults to CCC.store)
    # @return [HandleResult] supports destructuring: +cmd, reactor, events = result+
    # @raise [Sourced::ConcurrentAppendError] if conflicting messages found after history read
    # @raise [RuntimeError] if the decider raises a domain error (invariant violation)
    #
    # @example
    #   cmd = CourseApp::CreateCourse.new(payload: { course_id: 'c1', course_name: 'Algebra' })
    #   cmd, decider, events = Sourced::CCC.handle!(CourseApp::CourseDecider, cmd)
    #   if cmd.valid?
    #     # events were appended, offsets advanced
    #   else
    #     # cmd.errors has validation details
    #   end
    def self.handle!(reactor_class, command, store: nil)
      store ||= self.store

      partition_attrs = extract_partition_attrs(command, reactor_class)
      values = reactor_class.partition_keys.map { |k| partition_attrs[k]&.to_s }
      instance = reactor_class.new(values)

      unless command.valid?
        return HandleResult.new(command: command, reactor: instance, events: [])
      end

      # Load history if the reactor needs it (Deciders always do)
      needs_history = Injector.resolve_args(reactor_class, :handle_claim).include?(:history)
      if needs_history
        instance, read_result = load(reactor_class, store: store, **partition_attrs)
      end

      # Decide
      raw_events = instance.decide(command)
      correlated_events = raw_events.map { |e| command.correlate(e) }

      # Append command + events in one transaction with consistency guard
      guard = read_result&.guard
      to_append = [command] + correlated_events
      last_position = store.append(to_append, guard: guard)

      # Advance offsets for registered consumer groups
      advance_registered_offsets(store, reactor_class, partition_attrs, last_position)

      HandleResult.new(command: command, reactor: instance, events: correlated_events)
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
    # @param values [Hash{Symbol => String}] partition attribute values
    # @return [Array(reactor_instance, ReadResult)]
    #
    # @example
    #   decider, read_result = Sourced::CCC.load(MyDecider, course_id: 'Algebra', student_id: 'joe')
    #   decider.state       # evolved state
    #   read_result.guard   # ConsistencyGuard for subsequent appends
    def self.load(reactor_class, store: nil, **values)
      store ||= self.store
      partition_attrs = reactor_class.partition_keys.to_h { |k| [k, values[k]] }
      handled_types = reactor_class.handled_messages_for_evolve.map(&:type).uniq
      read_result = store.read_partition(partition_attrs, handled_types:)
      instance = reactor_class.new(values)

      instance.evolve(read_result.messages)

      [instance, read_result]
    end

    # Extract partition attribute values from a command's payload,
    # scoped to the reactor's declared partition_keys.
    #
    # @param command [CCC::Command]
    # @param reactor_class [Class]
    # @return [Hash{Symbol => String}]
    private_class_method def self.extract_partition_attrs(command, reactor_class)
      reactor_class.partition_keys.each_with_object({}) do |key, h|
        value = command.payload&.respond_to?(key) ? command.payload.send(key) : nil
        h[key] = value if value
      end
    end

    # Advance consumer group offsets for all reactors registered in the global router
    # that handle the given reactor_class's messages, so background workers skip
    # the already-handled command.
    #
    # @param store [CCC::Store]
    # @param reactor_class [Class]
    # @param partition_attrs [Hash{Symbol => String}]
    # @param position [Integer]
    private_class_method def self.advance_registered_offsets(store, reactor_class, partition_attrs, position)
      return unless config.router

      partition = partition_attrs.transform_keys(&:to_s)

      config.router.reactors.each do |registered_reactor|
        next unless registered_reactor == reactor_class

        store.advance_offset(
          registered_reactor.group_id,
          partition: partition,
          position: position
        )
      end
    end
  end
end

require 'sourced/ccc/configuration'
require 'sourced/ccc/message'
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
require 'sourced/ccc/command_context'
require 'sourced/ccc/topology'
require 'sourced/ccc/supervisor'
