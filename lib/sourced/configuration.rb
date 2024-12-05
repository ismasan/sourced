# frozen_string_literal: true

require 'console' #  comes with async gem
require 'sourced/types'
require 'sourced/backends/test_backend'

module Sourced
  class Configuration
    #  Backends must expose these methods
    BackendInterface = Types::Interface[
      :installed?,
      :reserve_next_for_reactor,
      :append_to_stream,
      :read_correlation_batch,
      :read_event_stream,
      :transaction
    ]

    attr_accessor :logger
    attr_reader :backend

    def initialize
      @logger = Console
      @backend = Backends::TestBackend.new
    end

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
