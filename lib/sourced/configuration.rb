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
      :transaction
    ]

    attr_accessor :logger
    attr_reader :backend, :error_strategy

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
  end
end
