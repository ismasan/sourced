# frozen_string_literal: true

require 'logger'
require 'sourced/error_strategy'
require 'sourced/async_executor'

module Sourced
  class Configuration
    StoreInterface = Types::Interface[
      :installed?,
      :install!,
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

    attr_accessor :logger, :worker_count, :batch_size,
                  :catchup_interval, :max_drain_rounds,
                  :claim_ttl_seconds, :housekeeping_interval,
                  :executor

    attr_reader :store, :router

    def initialize
      @logger = Logger.new($stdout)
      @worker_count = 2
      @batch_size = 50
      @catchup_interval = 5
      @max_drain_rounds = 10
      @claim_ttl_seconds = 120
      @housekeeping_interval = 30
      @executor = AsyncExecutor.new
      @store = nil
      @router = nil
      @error_strategy = ErrorStrategy.new
      @setup = false
    end

    # Accepts a Sourced::Store instance, a Sequel::SQLite::Database connection
    # (auto-wrapped in Sourced::Store.new(db)), or any object implementing StoreInterface.
    def store=(s)
      @store = case s.class.name
      when 'Sequel::SQLite::Database'
        require 'sourced/store'
        Store.new(s)
      else StoreInterface.parse(s)
      end
    end

    def error_strategy=(strategy)
      raise ArgumentError, 'Must respond to #call' unless strategy.respond_to?(:call)

      @error_strategy = strategy
    end

    attr_reader :error_strategy

    def setup!
      return if @setup

      unless @store
        require 'sourced/store'
        @store = Store.new(Sequel.sqlite)
      end
      @store.install!
      @router ||= Router.new(store: @store)
      @setup = true
    end
  end
end
