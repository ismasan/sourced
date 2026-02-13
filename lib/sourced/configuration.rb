# frozen_string_literal: true

require 'console' #  comes with async gem
require 'sourced/types'
require 'sourced/backends/test_backend'
require 'sourced/error_strategy'
require 'sourced/async_executor'

module Sourced
  # Configure a Sourced app.
  # @example
  #
  #  Sourced.configure do |config|
  #    config.backend = Sequel.Postgres('postgres://localhost/mydb')
  #    config.logger = Logger.new(STDOUT)
  #  end
  #
  class Configuration
    #  Backends must expose these methods
    BackendInterface = Types::Interface[
      :installed?,
      :reserve_next_for_reactor,
      :append_to_stream,
      :read_correlation_batch,
      :read_stream,
      :transaction,
      :stats,
      :updating_consumer_group,
      :register_consumer_group,
      :start_consumer_group,
      :stop_consumer_group,
      :reset_consumer_group
    ]

    # Interface that all executors must implement
    # @see AsyncExecutor
    # @see ThreadExecutor
    ExecutorInterface = Types::Interface[
      :start
    ]

    attr_accessor :logger
    # House-keeping configuration
    # interval: main loop tick for housekeeping (seconds)
    # heartbeat interval: how often to record heartbeats (seconds)
    # claim_ttl_seconds: how long before a worker is considered dead for claim release
    attr_accessor(
      :worker_count,
      :worker_batch_size,
      :housekeeping_count,
      :housekeeping_interval,
      :housekeeping_heartbeat_interval,
      :housekeeping_claim_ttl_seconds
    )

    attr_reader :backend, :executor

    def initialize
      @logger = Console
      @backend = Backends::TestBackend.new
      @error_strategy = ErrorStrategy.new
      @executor = AsyncExecutor.new
      @setup = false
      # Worker and house-keeping defaults
      @worker_count = 2
      @worker_batch_size = 1
      @housekeeping_count = 1
      @housekeeping_interval = 3
      @housekeeping_heartbeat_interval = 5
      @housekeeping_claim_ttl_seconds = 120
      @subscribers = []
    end

    def subscribe(callable = nil, &block)
      callable ||= block
      @subscribers << callable
      self
    end

    def setup!
      return if @setup

      @backend.setup!(self) if @backend.respond_to?(:setup!)
      @subscribers.each { |s| s.call(self) }
      @setup = true
    end

    # Configure the backend for the app.
    # Defaults to in-memory TestBackend
    # @param bnd [BackendInterface]
    def backend=(bnd)
      @backend = case bnd.class.name
      when 'Sequel::Postgres::Database', 'Sequel::SQLite::Database'
        require 'sourced/backends/sequel_backend'
        Sourced::Backends::SequelBackend.new(bnd)
      else
        BackendInterface.parse(bnd)
      end
    end

    # Configure the executor for the app.
    # Supports both symbol shortcuts and executor instances.
    # Defaults to AsyncExecutor for fiber-based concurrency.
    #
    # @param ex [Symbol, Object] The executor to use
    # @option ex [Symbol] :async Use AsyncExecutor (fiber-based, default)
    # @option ex [Symbol] :thread Use ThreadExecutor (thread-based)
    # @option ex [ExecutorInterface] Custom executor instance
    # @raise [ArgumentError] if executor doesn't implement ExecutorInterface
    #
    # @example Using symbol shortcuts
    #   config.executor = :async   # Default fiber-based
    #   config.executor = :thread  # Thread-based for CPU-intensive work
    #
    # @example Using custom executor
    #   config.executor = MyCustomExecutor.new
    def executor=(ex)
      @executor = case ex
      when :async
        AsyncExecutor.new
      when :thread
        require 'sourced/thread_executor'
        ThreadExecutor.new
      when ExecutorInterface
        ex
      else
        raise ArgumentError, "executor=(e) must support interface #{ExecutorInterface.inspect}"
      end
    end

    # Assign an error strategy
    # @param strategy [ErrorStrategy, #call(Exception, Sourced::Message, Group)]
    # @raise [ArgumentError] if strategy does not respond to #call
    def error_strategy=(strategy)
      raise ArgumentError, 'Must respond to #call(Exception, Sourced::Message, Group)' unless strategy.respond_to?(:call)

      @error_strategy = strategy
    end

    # Configure a built-in Sourced::ErrorStrategy
    # @example
    #   config.error_strategy do |s|
    #     s.retry(times: 30, after: 50, backoff: ->(retry_after, retry_count) { retry_after * retry_count })
    #
    #     s.on_retry do |n, exception, message, later| 
    #       puts "Retrying #{n} times" }
    #     end
    #
    #     s.on_stop do |exception, message|
    #       Sentry.capture_exception(exception)
    #     end
    #   end
    #
    # @yieldparam s [ErrorStrategy]
    def error_strategy(&blk)
      return @error_strategy unless block_given?

      self.error_strategy = ErrorStrategy.new(&blk)
    end
  end
end
