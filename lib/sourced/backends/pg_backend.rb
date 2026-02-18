# frozen_string_literal: true

require 'sourced/backends/sequel_backend'

module Sourced
  module Backends
    # Explicit PostgreSQL backend. Inherits all behaviour from SequelBackend
    # and overrides only the adapter setup to skip auto-detection.
    #
    # @example
    #   db = Sequel.postgres('myapp')
    #   backend = Sourced::Backends::PGBackend.new(db)
    class PGBackend < SequelBackend
      protected

      def setup_adapter
        @notifier = PGNotifier.new(db: @db)
      end
    end
  end
end
