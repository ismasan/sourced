# frozen_string_literal: true

require 'sourced'

module Sequel
  # How an SQLite database waits for a locked database, and what it raises
  # when the wait times out.
  #
  # Connections wait with sqlite3's +busy_handler_timeout+, which sleeps in Ruby
  # between retries, instead of Sequel's +busy_timeout+, which sleeps inside
  # SQLite. A fiber waiting for a lock another connection holds then yields to
  # the fiber scheduler, where inside SQLite it would block every fiber on its
  # thread until the lock is released or the wait times out. The timeout is the
  # database's +:timeout+ option (Sequel's default, 5000ms). A wait that times
  # out raises {Sourced::Store::BusyError}. Applies to connections already open.
  #
  # @example
  #   db = Sequel.sqlite('app.db', timeout: 10_000)
  #   db.extension(:sourced_sqlite_busy_wait)
  module SourcedSQLiteBusyWait
    DEFAULT_TIMEOUT = 5000

    # SQLITE_BUSY, SQLITE_BUSY_RECOVERY and SQLITE_BUSY_TIMEOUT. SQLITE_BUSY_SNAPSHOT
    # (517) stays Sequel's SerializationFailure: it isn't a wait that timed out.
    BUSY_CODES = [5, 261, 773].freeze

    def self.extended(db)
      db.pool.all_connections { |conn| db.send(:wait_when_busy, conn) }
    end

    def connect(server)
      wait_when_busy(super)
    end

    private

    def wait_when_busy(conn)
      conn.busy_handler_timeout = typecast_value_integer(opts.fetch(:timeout, DEFAULT_TIMEOUT))
      conn
    end

    def database_specific_error_class(exception, opts)
      BUSY_CODES.include?(sqlite_error_code(exception)) ? Sourced::Store::BusyError : super
    end
  end

  Database.register_extension(:sourced_sqlite_busy_wait, SourcedSQLiteBusyWait)
end
