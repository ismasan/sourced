# frozen_string_literal: true

require 'console' #  comes with async gem
require 'sors/types'
require 'sors/backends/test_backend'

module Sors
  class Configuration
    #  Backends must expose these methods
    BackendInterface = Types::Interface[
      :schedule_commands,
      :reserve_next,
      :append_events,
      :read_event_batch,
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
                   require 'sors/backends/sequel_backend'
                   Sors::Backends::SequelBackend.new(bnd)
                 else
                   BackendInterface.parse(bnd)
                 end
    end
  end
end
