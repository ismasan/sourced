# frozen_string_literal: true

require 'console' #  comes with async gem
require 'sourced/types'
require 'sourced/backends/test_backend'
require 'sourced/error_strategy'

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
      :read_event_stream,
      :schedule_commands,
      :next_command,
      :transaction,
      :stats,
      :updating_consumer_group,
      :register_consumer_group,
      :start_consumer_group,
      :stop_consumer_group,
      :reset_consumer_group
    ]

    attr_accessor :logger
    attr_reader :backend

    def initialize
      @logger = Console
      @backend = Backends::TestBackend.new
      @error_strategy = ErrorStrategy.new
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
