# frozen_string_literal: true

require 'logger'
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

  # Raised by {.handle!} when the reactor is registered with Sourced but its
  # consumer group isn't in the store, which registers groups when Sourced starts.
  ConsumerGroupNotRegisteredError = Class.new(Error)

  # Default logger for components built outside a configured system, ex. in specs.
  NULL_LOGGER = Logger.new(nil)

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

  # Sourced's configuration: the root of a tree of typed components, with
  # defaults, dependencies and a lifecycle (see {Config} for the tree).
  #
  #   Sourced.config.config!('workers.count') { 4 }
  #   Sourced.config.component!('db') { build { Sequel.sqlite('app.db') }; teardown(&:disconnect) }
  #   Sourced.start!
  #
  # A host app with its own component tree mounts it instead, and boots it with
  # the rest of the app:
  #
  #   App.mount('sourced', Sourced)
  #   App.config!('sourced.db', ['db']) { |db| db }
  #   App.start!
  #
  # @return [Sourced::Component]
  def self.config
    @config ||= Config.build
  end

  # Mountable: +App.mount('sourced', Sourced)+ mounts {.config}
  # @return [Sourced::Component]
  def self.to_component = config

  # Yields {.config}, for configuring a standalone Sourced in a block.
  # @yieldparam config [Sourced::Component]
  # @return [Sourced::Component]
  def self.configure
    yield config
    config
  end

  # Register a reactor, as a component under +reactors+ in {.config}.
  # Must be called before the configuration is prepared or booted.
  # @param reactor [Class] a reactor (see {Router#register} for the protocol)
  # @return [Sourced::Component] the reactor's node
  # @raise [ArgumentError] if a reactor is already registered under the same key (its group_id, escaped)
  # @raise [Sourced::Component::LockedComponentError] once the configuration is prepared
  def self.register(reactor)
    Config.register(config, reactor)
  end

  # Boot a standalone Sourced: build every component, set up the store and
  # consumer groups, and spawn workers into +context+. By default workers run
  # in threads and this returns; pass an Async::Task to run them as fibers in
  # its reactor, or set +workers.count+ to 0 to run none in this process.
  # To block until the process is signalled, see {Supervisor}.
  # A mounted Sourced is booted by its host's root.
  # @return [Sourced::Component]
  def self.start!(context = ThreadExecutor.new)
    config.start!(context)
  end

  # Stop workers and tear down every component, in reverse dependency order.
  # @return [Sourced::Component]
  def self.teardown!
    config.teardown!
  end

  # @return [Sourced::Store]
  # @raise [Sourced::Component::NotBuiltError] until the configuration is built
  def self.store = config['store']

  # @return [Sourced::Router]
  # @raise [Sourced::Component::NotBuiltError] until the configuration is built
  def self.router = config['router']

  # The message-flow graph of every registered reactor (see {Topology}).
  # Built on each call, from the reactors' source: keep the result rather than calling it repeatedly.
  # @return [Array]
  # @raise [Sourced::Component::NotBuiltError] until the configuration is built
  def self.topology = config['topology']

  # @return [Logger]
  def self.logger = config['logger']

  def self.stop_consumer_group(reactor_or_id, message = nil)
    router.stop_consumer_group(reactor_or_id, message)
  end

  def self.reset_consumer_group(reactor_or_id)
    router.reset_consumer_group(reactor_or_id)
  end

  def self.start_consumer_group(reactor_or_id)
    router.start_consumer_group(reactor_or_id)
  end

  # Drop the configuration, so the next {.config} builds a fresh one. For specs.
  def self.reset!
    @config = nil
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
    partition_attrs = extract_partition_attrs(command, reactor_class)
    values = reactor_class.partition_keys.map { |k| partition_attrs[k]&.to_s }

    unless command.valid?
      return HandleResult.new(command: command, reactor: reactor_class.new(values), events: [])
    end

    store ||= self.store
    needs_history = Injector.resolve_args(reactor_class, :handle_claim).include?(:history)
    if needs_history
      instance, read_result = load(reactor_class, store: store, **partition_attrs)
    else
      instance = reactor_class.new(values)
    end

    raw_events = instance.decide(command)
    correlated_events = raw_events.map { |e| command.correlate(e) }

    guard = read_result&.guard
    to_append = [command] + correlated_events
    last_position = store.append(to_append, guard: guard)

    # nil when the command itself was future-dated and scheduled: nothing to skip past.
    advance_registered_offsets(store, reactor_class, partition_attrs, last_position) if last_position

    HandleResult.new(command: command, reactor: instance, events: correlated_events)
  end

  # Load a reactor instance from its event history using AND-filtered partition reads.
  # Pass +upto:+ (partition-local rank) to evolve over at most the first N matching
  # messages in the partition — useful for time-travel, deterministic replay, or debugging.
  def self.load(reactor_class, store: nil, upto: nil, **values)
    store ||= self.store
    partition_attrs = reactor_class.partition_keys.to_h { |k| [k, values[k]] }
    handled_types = reactor_class.handled_messages_for_evolve.map(&:type).uniq
    read_result = store.read_partition(partition_attrs, handled_types:, upto: upto)
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

  # Skip the handled command for a registered reactor's workers. The group
  # exists once Sourced has started; before that, advancing would silently do
  # nothing and a worker would decide the command again later.
  private_class_method def self.advance_registered_offsets(store, reactor_class, partition_attrs, position)
    return unless config.declared?(Config.reactor_key(reactor_class))

    advanced = store.advance_offset(
      reactor_class.group_id,
      partition: partition_attrs.transform_keys(&:to_s),
      position: position
    )
    return if advanced

    raise ConsumerGroupNotRegisteredError,
          "#{reactor_class} is registered with Sourced, but its consumer group " \
          "#{reactor_class.group_id.inspect} is not in the store: start Sourced before handling " \
          'commands, or handle them with the store it is registered in'
  end
end

require 'sourced/config'
require 'sourced/thread_executor'
require 'sourced/store'
require 'sourced/message'
require 'sourced/message_ext'
require 'sourced/actions'
require 'sourced/action_runner'
require 'sourced/reactor_defaults'
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
