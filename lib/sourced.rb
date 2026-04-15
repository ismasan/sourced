# frozen_string_literal: true

require_relative 'sourced/version'
require 'sourced/types'
require 'sourced/injector'

# Sourced is an event-sourcing library for Ruby built around stream-less,
# partition-based consistency. Events go into a flat, globally-ordered log and
# consistency context is assembled dynamically by querying relevant facts via
# key-value pairs extracted from event payloads.
module Sourced
  # Base error class for all Sourced-specific exceptions
  class Error < StandardError; end

  ConcurrentAppendError = Class.new(Error)
  UnknownMessageError = Class.new(ArgumentError)
  PastMessageDateError = Class.new(ArgumentError)

  # Raised when a batch is partially processed before a message raises.
  # Carries the action_pairs for successfully processed messages,
  # the failed message, and the original exception as #cause.
  class PartialBatchError < Error
    attr_reader :action_pairs, :failed_message

    def initialize(action_pairs, failed_message, cause)
      @action_pairs = action_pairs
      @failed_message = failed_message
      super(cause.message)
      set_backtrace(cause.backtrace)
    end
  end

  # @return [Configuration] the global Sourced configuration instance
  def self.config
    @config ||= Configuration.new
  end

  # Configure the Sourced module. Stores the block for re-running after fork
  # (see {.setup!}), then runs it immediately.
  # @yieldparam config [Configuration]
  def self.configure(&block)
    @configure_block = block
    setup!
  end

  # Run (or re-run) the configure block on a fresh Configuration.
  # Safe to call after a process fork to re-establish database connections.
  def self.setup!
    @config = Configuration.new
    @configure_block&.call(@config)
    @config.setup!
    @config.freeze
  end

  # Register a reactor class with the global router.
  def self.register(reactor)
    config.setup!
    config.router.register(reactor)
  end

  # @return [Sourced::Store]
  def self.store
    config.setup!
    config.store
  end

  # @return [Sourced::Router]
  def self.router
    config.setup!
    config.router
  end

  def self.stop_consumer_group(reactor_or_id, message = nil)
    config.router.stop_consumer_group(reactor_or_id, message)
  end

  def self.reset_consumer_group(reactor_or_id)
    config.router.reset_consumer_group(reactor_or_id)
  end

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
  # the global {.router}.
  def self.topology
    @topology ||= Topology.build(router.reactors)
  end

  def self.reset_topology
    @topology = nil
  end

  # Generate a standardized method name for message handlers.
  # @api private
  def self.message_method_name(prefix, name)
    "__handle_#{prefix}_#{name.split('::').map(&:downcase).join('_')}"
  end

  # Returned by {.handle!} with command, reactor instance, and new events.
  HandleResult = Data.define(:command, :reactor, :events) do
    def to_ary = [command, reactor, events]
  end

  # Handle a command synchronously: validate, load history, decide, append, ACK.
  def self.handle!(reactor_class, command, store: nil)
    store ||= self.store

    partition_attrs = extract_partition_attrs(command, reactor_class)
    values = reactor_class.partition_keys.map { |k| partition_attrs[k]&.to_s }
    instance = reactor_class.new(values)

    unless command.valid?
      return HandleResult.new(command: command, reactor: instance, events: [])
    end

    needs_history = Injector.resolve_args(reactor_class, :handle_claim).include?(:history)
    if needs_history
      instance, read_result = load(reactor_class, store: store, **partition_attrs)
    end

    raw_events = instance.decide(command)
    correlated_events = raw_events.map { |e| command.correlate(e) }

    guard = read_result&.guard
    to_append = [command] + correlated_events
    last_position = store.append(to_append, guard: guard)

    advance_registered_offsets(store, reactor_class, partition_attrs, last_position)

    HandleResult.new(command: command, reactor: instance, events: correlated_events)
  end

  # Load a reactor instance from its event history using AND-filtered partition reads.
  def self.load(reactor_class, store: nil, **values)
    store ||= self.store
    partition_attrs = reactor_class.partition_keys.to_h { |k| [k, values[k]] }
    handled_types = reactor_class.handled_messages_for_evolve.map(&:type).uniq
    read_result = store.read_partition(partition_attrs, handled_types:)
    instance = reactor_class.new(values)

    instance.evolve(read_result.messages)

    [instance, read_result]
  end

  private_class_method def self.extract_partition_attrs(command, reactor_class)
    reactor_class.partition_keys.each_with_object({}) do |key, h|
      value = command.payload&.respond_to?(key) ? command.payload.send(key) : nil
      h[key] = value if value
    end
  end

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

require 'sourced/configuration'
require 'sourced/message'
require 'sourced/actions'
require 'sourced/consumer'
require 'sourced/evolve'
require 'sourced/react'
require 'sourced/sync'
require 'sourced/decider'
require 'sourced/projector'
require 'sourced/router'
require 'sourced/worker'
require 'sourced/stale_claim_reaper'
require 'sourced/dispatcher'
require 'sourced/command_context'
require 'sourced/topology'
require 'sourced/supervisor'
require 'sourced/durable_workflow'
