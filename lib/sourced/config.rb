# frozen_string_literal: true

require 'logger'
require 'sequel'
require 'sourced/component'
require 'sourced/error_strategy'
require 'sourced/async_executor'
require 'sourced/inline_notifier'

module Sourced
  # Sourced's configuration: a tree of typed components (see sourced-component),
  # with defaults, dependencies and a lifecycle.
  #
  #   config = Sourced::Config.build
  #   config.config!('workers.count') { 4 }
  #   Sourced::Config.register(config, Courses)
  #   config.start!(task)
  #
  # The tree:
  #
  #   logger            Logger.new($stdout)
  #   db                in-memory SQLite. Disconnected on teardown
  #   notifier          InlineNotifier
  #   executor          AsyncExecutor
  #   error_strategy    ErrorStrategy
  #   store             Store over db
  #   reactors.*        one component per registered reactor (see .register)
  #   router            routes to reactors.*. On start, sets up the store, registers
  #                     consumer groups and freezes the error strategy (see Router#setup!)
  #   topology          message-flow graph of reactors.*
  #   workers.*         count, batch_size, max_drain_rounds, catchup_interval
  #   housekeeping.*    interval, claim_ttl_seconds
  #   dispatcher        spawns workers into the context passed to #start!. Stopped on teardown
  #
  # Building only constructs objects: nothing touches the database until #start!.
  module Config
    T = Sourced::Component::T

    StoreInterface = T::Interface[
      # #setup! is how a store prepares itself on start (see Store#setup!).
      # Deliberately generic: creating tables and compiling codecs are one
      # store's answer to it, not part of the contract.
      :setup!,
      :append,
      :read,
      :read_partition,
      :claim_next,
      :ack,
      :release,
      :register_consumer_group,
      :worker_heartbeat,
      :release_stale_claims,
      :notifier
    ]

    # What a store notifier must respond to. {InlineNotifier} is the reference
    # implementation; see it for the semantics of each method.
    NotifierInterface = T::Interface[
      :subscribe,
      :notify_new_messages,
      :notify_reactor_resumed,
      :start,
      :stop
    ]

    # Reactors are duck-typed (see Router#register)
    ReactorInterface = T::Interface[:handled_messages, :handle_claim]

    LoggerInterface = T::Interface[:debug, :info, :warn, :error]
    ExecutorInterface = T::Interface[:start, :new_queue]
    ErrorStrategyInterface = T::Interface[:call]

    # Characters with a meaning in component keys
    KEY_SEPARATORS = /[.*]/

    # A fresh, open configuration tree, with defaults for every component.
    # @return [Sourced::Component]
    def self.build
      Sourced::Component.new.tap do |c|
        c.declare('logger', LoggerInterface) { Logger.new($stdout) }

        c.declare('db', Sequel::Database)
        c.component!('db') do
          build { Sequel.sqlite }
          teardown(&:disconnect)
        end

        c.declare('notifier', NotifierInterface) { InlineNotifier.new }
        c.declare('executor', ExecutorInterface) { AsyncExecutor.new }

        c.declare('error_strategy', ErrorStrategyInterface) { ErrorStrategy.new }

        c.declare('store', StoreInterface)
        c.component!('store', %w[db notifier logger]) do
          build { |db, notifier, logger| Store.new(db, notifier:, logger:) }
        end

        # The router's start sets up the store and freezes the error strategy
        # (see Router#setup!), so overriding either only needs to build it:
        # re-implementing a component replaces its hooks.
        c.declare('router', Router)
        c.component!('router', %w[store reactors.* error_strategy]) do
          build do |store, reactors, error_strategy|
            Router.new(store:, reactors: reactors.values, error_strategy:)
          end
          start { |router, _| router.setup! }
        end

        c.declare('topology', T::Array)
        c.config!('topology', ['reactors.*']) { |reactors| Topology.build(reactors.values) }

        c.declare('workers.count', T::Integer[0..]) { 2 }
        c.declare('workers.batch_size', T::Integer[1..]) { 50 }
        c.declare('workers.max_drain_rounds', T::Integer[1..]) { 10 }
        c.declare('workers.catchup_interval', T::Numeric) { 5 }
        c.declare('housekeeping.interval', T::Numeric) { 30 }
        c.declare('housekeeping.claim_ttl_seconds', T::Integer[1..]) { 120 }

        c.declare('dispatcher', Dispatcher)
        c.component!('dispatcher', %w[
          router executor logger
          workers.count workers.batch_size workers.max_drain_rounds workers.catchup_interval
          housekeeping.interval housekeeping.claim_ttl_seconds
        ]) do
          build do |router, executor, logger, count, batch_size, max_drain_rounds, catchup_interval, housekeeping_interval, claim_ttl_seconds|
            Dispatcher.new(
              router:,
              executor:,
              logger:,
              worker_count: count,
              batch_size:,
              max_drain_rounds:,
              catchup_interval:,
              housekeeping_interval:,
              claim_ttl_seconds:
            )
          end
          start { |dispatcher, context| dispatcher.start(context) }
          teardown(&:stop)
        end
      end
    end

    # Register a reactor as a component under +reactors+, which the router and
    # topology depend on. Keyed by the reactor's group_id, so registering two
    # reactors with the same group_id raises.
    #
    # @param config [Sourced::Component] a tree built by {.build}
    # @param reactor [Class] a reactor (see {ReactorInterface})
    # @return [Sourced::Component] the reactor's node
    # @raise [ArgumentError] if a reactor with the same group_id is already registered
    # @raise [Sourced::Component::LockedComponentError] once the tree is prepared
    def self.register(config, reactor)
      ReactorDefaults.apply(reactor)
      key = reactor_key(reactor)
      if config.declared?(key)
        raise ArgumentError, "can't register #{reactor}: a reactor with group_id #{reactor.group_id.inspect} is already registered"
      end

      config.declare(key, ReactorInterface)
      config.config!(key) { reactor }
      config.node(key)
    end

    # The component key of a reactor, ex. 'reactors.CourseDecider'
    # @return [String]
    def self.reactor_key(reactor)
      "reactors.#{reactor.group_id.to_s.gsub(KEY_SEPARATORS, '_')}"
    end
  end
end
