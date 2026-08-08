# frozen_string_literal: true

require 'logger'
require 'sourced/error_strategy'
require 'sourced/async_executor'

module Sourced
  class Configuration
    StoreInterface = Types::Interface[
      # #setup! is how a store prepares itself at boot (see Store#setup!).
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

    attr_accessor :logger, :worker_count, :batch_size,
                  :catchup_interval, :max_drain_rounds,
                  :claim_ttl_seconds, :housekeeping_interval,
                  :executor

    attr_reader :store, :router

    # The wire format messages are serialized with — a {Plumb::Codec} subclass,
    # global to the app.
    # @see #codec=
    # @return [Class<Plumb::Codec>]
    attr_reader :codec

    # The mutable error strategy. Configure retry policy and register callbacks
    # directly on it (possibly from different layers); it freezes when the
    # configuration is frozen (see {#freeze}).
    # @return [ErrorStrategy, #call]
    attr_reader :error_strategy

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
      @codec = Plumb::Codec::JSON
      @setup = false
    end

    # Accepts a Sourced::Store instance, a Sequel::SQLite::Database connection
    # (auto-wrapped in Sourced::Store.new(db)), or any object implementing StoreInterface.
    def store=(s)
      @store = case s.class.name
      when 'Sequel::SQLite::Database'
        require 'sourced/store'
        Store.new(s, codec: @codec)
      else StoreInterface.parse(s)
      end
    end

    # Set the wire format. Expects a {Plumb::Codec} subclass — normally a
    # subclass of {Plumb::Codec::JSON} adding encoders for the app's own types:
    #
    #   class MyCodec < Plumb::Codec::JSON
    #     encoder MoneyEncoder
    #   end
    #
    #   Sourced.configure { |config| config.codec = MyCodec }
    #
    # Handed to an already-built store as well, so this and {#store=} can be set
    # in either order. What a store does with it is its own business: {Store}
    # compiles payload pairs from it.
    #
    # @param codec_class [Class<Plumb::Codec>]
    def codec=(codec_class)
      @codec = codec_class
      @store.codec = codec_class if @store.respond_to?(:codec=)
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
        @store = Store.new(Sequel.sqlite, codec: @codec)
      end
      # Whatever this store needs to be usable: {Store} creates its tables and
      # compiles its serializer, so a message type it can't persist fails here.
      @store.setup!
      @router ||= Router.new(store: @store)
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
