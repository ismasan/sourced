# frozen_string_literal: true

require 'logger'
require 'sequel'
require 'sourced/component'
require 'sourced/error_strategy'
require 'sourced/async_executor'
require 'sourced/inline_notifier'
require 'sourced/installer'

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
  #   executor          AsyncExecutor. Runs workers under Sourced::Supervisor
  #   error_strategy    ErrorStrategy
  #   store             Store over db. Compiles its codec on prepare, and installs its tables on start
  #   store.table_prefix  prefix of the store's table names, ex. 'sourced' => sourced_messages
  #   reactors.*        one component per registered reactor (see .register)
  #   router            routes to reactors.*. On start, registers consumer groups and
  #                     freezes the error strategy (see Router#setup!)
  #   topology          message-flow graph of reactors.*, built on each read
  #   workers.*         count, batch_size, max_drain_rounds, catchup_interval, shutdown_timeout
  #   housekeeping.*    interval, claim_ttl_seconds
  #   dispatcher        spawns workers into the context passed to #start!. On stop (and so on
  #                     teardown), stops and waits for workers to finish their batches (up to
  #                     shutdown_timeout, then raises Dispatcher::ShutdownTimeoutError). Can be
  #                     deferred, and started and stopped by key
  #
  # Building only constructs objects: nothing touches the database until #start!.
  # Preparing compiles the store's codec, so it can run once before forking.
  module Config
    T = Sourced::Component::T

    # What the rest of Sourced uses a store for. How a store gets ready (tables,
    # codecs, connections) is part of its own lifecycle, not this contract: the
    # store component's hooks own it, so a component implementing another store
    # brings its own.
    StoreInterface = T::Interface[
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
    # implementation; see it for the semantics of each method. The dispatcher
    # calls +start+ on each run and +stop+ when it stops, so a notifier must
    # support starting again after stopping (see Dispatcher#start).
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
    ExecutorInterface = T::Interface[:start]
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

        # Overriding db (ex. a file-backed SQLite) keeps this lifecycle. Re-implementing
        # store replaces it, along with its hooks: the new store brings its own.
        c.declare('store', StoreInterface)
        c.declare('store.table_prefix', Installer::TablePrefix) { 'sourced' }
        c.component!('store', %w[db notifier logger store.table_prefix]) do
          # Compiles the codec stores are built with, which only needs the message
          # types, so a message type the store can't persist fails the boot before
          # anything connects. A process that prepares before forking shares the
          # compiled codec with its children.
          prepare { Store::MessageCodec.default.compile! }
          build { |db, notifier, logger, prefix| Store.new(db, notifier:, logger:, prefix:) }
          # Creates the tables (and compiles the store's codec, if it was given another)
          start { |store, _| store.setup! }
        end

        # The router's start freezes the error strategy (see Router#setup!), so an
        # override only needs to build it.
        c.declare('router', Router)
        c.component!('router', %w[store reactors.* error_strategy]) do
          build do |store, reactors, error_strategy|
            Router.new(store:, reactors: reactors.values, error_strategy:)
          end
          start { |router, _| router.setup! }
        end

        # Dynamic: built when read, as it parses the reactors' source. Most processes never read it
        c.declare('topology', T::Array)
        c.config('topology', ['reactors.*']) { |reactors| Topology.build(reactors.values) }

        c.declare('workers.count', T::Integer[0..]) { 2 }
        c.declare('workers.batch_size', T::Integer[1..]) { 50 }
        c.declare('workers.max_drain_rounds', T::Integer[1..]) { 10 }
        c.declare('workers.catchup_interval', T::Numeric) { 5 }
        c.declare('workers.shutdown_timeout', T::Numeric) { 30 }
        c.declare('housekeeping.interval', T::Numeric) { 30 }
        c.declare('housekeeping.claim_ttl_seconds', T::Integer[1..]) { 120 }

        # Restartable: a host can defer it and start and stop it by key, ex. only
        # while its process is the elected leader
        c.declare('dispatcher', Dispatcher)
        c.component!('dispatcher', %w[router logger workers.* housekeeping.*]) do
          build do |router, logger, workers, housekeeping|
            Dispatcher.new(
              router:,
              logger:,
              worker_count: workers['count'],
              batch_size: workers['batch_size'],
              max_drain_rounds: workers['max_drain_rounds'],
              catchup_interval: workers['catchup_interval'],
              shutdown_timeout: workers['shutdown_timeout'],
              housekeeping_interval: housekeeping['interval'],
              claim_ttl_seconds: housekeeping['claim_ttl_seconds']
            )
          end
          start { |dispatcher, context| dispatcher.start(context) }
          stop(&:stop!)
        end
      end
    end

    # Register a reactor as a component under +reactors+, which the router and
    # topology depend on, keyed by its group_id (see {.reactor_key}).
    # The router checks that group_ids are unique.
    #
    # @param config [Sourced::Component] a tree built by {.build}
    # @param reactor [Class] a reactor (see {ReactorInterface})
    # @return [Sourced::Component] the reactor's node
    # @raise [ArgumentError] if a reactor is already registered under the same key
    # @raise [Sourced::Component::LockedComponentError] once the tree is prepared
    def self.register(config, reactor)
      ReactorDefaults.apply(reactor)
      key = reactor_key(reactor)
      begin
        config.declare(key, ReactorInterface)
      rescue Sourced::Component::DeclarationOverrideError
        raise ArgumentError, "can't register #{reactor} (group_id #{reactor.group_id.inspect}) as #{key}: " \
                             "a reactor is already registered under that key. Keys are group_ids with '.' and '*' replaced by '_'"
      end
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
