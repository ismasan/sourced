# frozen_string_literal: true

module Sourced
  class Store
    # Raised for a write that timed out waiting for SQLite's lock, or for its
    # turn among the process's writes: transient (see {Sourced::StoreBusyError}).
    # A +Sequel::DatabaseLockTimeout+, so rescues of Sequel's errors still catch it.
    class BusyError < Sequel::DatabaseLockTimeout
      include Sourced::StoreBusyError
    end

    # How the store's SQLite database waits for its single writer's lock: the
    # Sequel extensions +sourced_sqlite_busy_wait+ (wait in Ruby, so other fibers
    # run) and +sourced_sqlite_write_queue+ (the process's writes take turns, so
    # one at a time waits for SQLite's lock). The store component applies them
    # on start (see Config.build). Apps can apply them to databases of their own
    # by name.
    #
    # @example
    #   db = Sequel.sqlite('app.db', timeout: 10_000)
    #   Sourced::Store::BusyHandler.apply(db)
    module BusyHandler
      EXTENSIONS = %i[sourced_sqlite_busy_wait sourced_sqlite_write_queue].freeze

      # Apply {EXTENSIONS} to an SQLite database (once: Sequel loads an
      # extension into a database only once); other databases are left alone.
      # @param db [Sequel::Database]
      # @return [Sequel::Database] +db+
      def self.apply(db)
        db.extension(*EXTENSIONS) if db.adapter_scheme == :sqlite
        db
      end
    end
  end
end
