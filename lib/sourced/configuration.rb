# frozen_string_literal: true

require 'logger'
require 'sourced/error_strategy'
require 'sourced/async_executor'
require 'sourced/inline_notifier'
require 'sourced/config'

module Sourced
  class Configuration
    StoreInterface = Config::StoreInterface
    NotifierInterface = Config::NotifierInterface

    attr_accessor :logger, :worker_count, :batch_size,
                  :catchup_interval, :max_drain_rounds,
                  :claim_ttl_seconds, :housekeeping_interval,
                  :executor

    attr_reader :store, :router

    # The mutable error strategy. Configure retry policy and register callbacks
    # directly on it (possibly from different layers); it freezes when the
    # configuration is frozen (see {#freeze}).
    # @return [ErrorStrategy, #call]
    attr_reader :error_strategy

    # The notifier stores announce appends and resumed reactors through, and
    # the dispatcher listens on. Stores built without an explicit +notifier:+
    # resolve this one on every call (see {Store#notifier}), so it can be
    # assigned before or after the store, and it survives {#disconnect!} —
    # like {#error_strategy}, it is configuration, not a connection.
    #
    # The default {InlineNotifier} is in-process. Assign one that crosses
    # process boundaries when appends happen in processes other than the
    # one running the dispatcher.
    #
    # @return [#subscribe, #notify_new_messages, #notify_reactor_resumed, #start, #stop]
    attr_reader :notifier

    def initialize
      @logger = Logger.new($stdout)
      @notifier = InlineNotifier.new
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
        Store.new(s, logger:)
      else StoreInterface.parse(s)
      end
    end

    # @param notifier [Object] must implement {NotifierInterface}
    def notifier=(notifier)
      @notifier = NotifierInterface.parse(notifier)
    end

    def error_strategy=(strategy)
      raise ArgumentError, 'Must respond to #call' unless strategy.respond_to?(:call)

      @error_strategy = strategy
    end

    # Deep-freeze: also freeze the error strategy so it can't be reconfigured
    # once the configuration is finalized.
    # @return [self]
    def freeze
      @error_strategy.freeze
      super
    end

    def setup!
      return if @setup

      unless @store
        require 'sourced/store'
        @store = Store.new(Sequel.sqlite, logger:)
      end
      # Whatever this store needs to be usable: {Store} creates its tables and
      # compiles its serializer, so a message type it can't persist fails here.
      @store.setup!
      # Resolve the strategy on every error, so one assigned after setup still applies.
      @router ||= Router.new(store: @store, error_strategy: ->(*args) { @error_strategy.call(*args) })
      @setup = true
    end

    # Drop the store and router and mark the configuration as un-setup, so the
    # next {#setup!} (after re-running the configure block) establishes fresh
    # database connections. Used by {Sourced.setup!} after a process fork.
    # @return [self]
    def disconnect!
      @store = nil
      @router = nil
      @setup = false
      self
    end
  end
end
