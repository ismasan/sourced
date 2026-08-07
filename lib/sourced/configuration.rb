# frozen_string_literal: true

require 'logger'
require 'sourced/error_strategy'
require 'sourced/async_executor'
require 'sourced/message_codec'

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

    # Serializes messages to and from the store's JSON columns, using {#codec}.
    # @see #codec=
    # @return [MessageCodec]
    attr_reader :message_codec

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
      @message_codec = MessageCodec.default
      @setup = false
    end

    # Accepts a Sourced::Store instance, a Sequel::SQLite::Database connection
    # (auto-wrapped in Sourced::Store.new(db)), or any object implementing StoreInterface.
    def store=(s)
      @store = case s.class.name
      when 'Sequel::SQLite::Database'
        require 'sourced/store'
        Store.new(s, message_codec: @message_codec)
      else StoreInterface.parse(s)
      end
    end

    # Set the wire format used to serialize message payloads. Expects a
    # {Plumb::Codec} subclass — normally a subclass of {Sourced::Codec} adding
    # encoders for the app's own types:
    #
    #   class MyCodec < Sourced::Codec
    #     encoder MoneyEncoder
    #   end
    #
    #   Sourced.configure { |config| config.codec = MyCodec }
    #
    # Pushes the new codec at an already-built store, so this and {#store=} can
    # be set in either order.
    #
    # @param codec_class [Class<Plumb::Codec>]
    def codec=(codec_class)
      self.message_codec = MessageCodec.new(codec_class)
    end

    # @return [Class<Plumb::Codec>] the wire format messages are serialized with
    def codec = @message_codec.codec

    # Install a preconfigured {MessageCodec} — e.g. one scoped to its own
    # message registry. {#codec=} is the usual way in.
    # @param message_codec [MessageCodec]
    def message_codec=(message_codec)
      @message_codec = message_codec
      @store.message_codec = @message_codec if @store.respond_to?(:message_codec=)
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
        @store = Store.new(Sequel.sqlite, message_codec: @message_codec)
      end
      @store.install!
      @router ||= Router.new(store: @store)
      compile_codecs!
      @setup = true
    end

    # Compile codecs for every message class defined so far: warms the cache so
    # no request pays for it, and fails the boot if any message type can't be
    # serialized by the configured codec (see {MessageCodec#compile!}).
    # @return [void]
    # @raise [Plumb::TypeError] naming the message type and attribute
    private def compile_codecs! = @message_codec.compile!

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
