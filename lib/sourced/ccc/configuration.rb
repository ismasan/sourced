# frozen_string_literal: true

module Sourced
  module CCC
    class Configuration
      attr_accessor :logger, :worker_count, :batch_size,
                    :catchup_interval, :max_drain_rounds,
                    :claim_ttl_seconds, :housekeeping_interval

      attr_reader :store, :router

      def initialize
        @logger = Sourced.config.logger
        @worker_count = 2
        @batch_size = 50
        @catchup_interval = 5
        @max_drain_rounds = 10
        @claim_ttl_seconds = 120
        @housekeeping_interval = 30
        @store = nil
        @router = nil
        @error_strategy = nil
        @setup = false
      end

      # Accepts either a CCC::Store instance or a Sequel::SQLite::Database connection.
      # When given a DB connection, wraps it in CCC::Store.new(db).
      def store=(s)
        @store = case s
        when Store then s
        else Store.new(s)
        end
      end

      def error_strategy=(strategy)
        raise ArgumentError, 'Must respond to #call' unless strategy.respond_to?(:call)

        @error_strategy = strategy
      end

      def error_strategy
        @error_strategy || Sourced.config.error_strategy
      end

      def setup!
        return if @setup

        @store ||= Store.new(Sequel.sqlite)
        @store.install!
        @router ||= Router.new(store: @store)
        @setup = true
      end
    end
  end
end
