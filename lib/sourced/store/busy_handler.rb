# frozen_string_literal: true

module Sourced
  class Store
    # Raised for a statement that timed out waiting for SQLite's lock: transient
    # (see {Sourced::StoreBusyError}). A +Sequel::DatabaseLockTimeout+, so
    # rescues of Sequel's errors still catch it.
    class BusyError < Sequel::DatabaseLockTimeout
      include Sourced::StoreBusyError
    end

    # How a Sequel SQLite database waits for a locked database, and what it
    # raises when the wait times out. {.apply} extends a database with it; the
    # store component does that on start.
    #
    # Connections wait with sqlite3's +busy_handler_timeout+, which sleeps in Ruby
    # between retries, instead of Sequel's +busy_timeout+, which sleeps inside
    # SQLite. A fiber waiting for a lock another process holds then yields to the
    # fiber scheduler, where inside SQLite it would block every fiber on its
    # thread until the lock is released or the wait times out. The timeout is
    # the database's +:timeout+ option (Sequel's default, 5000ms). A wait that
    # times out raises {BusyError}.
    #
    # @example
    #   db = Sequel.sqlite('app.db', timeout: 10_000)
    #   Sourced::Store::BusyHandler.apply(db)
    module BusyHandler
      DEFAULT_TIMEOUT = 5000

      # SQLITE_BUSY, SQLITE_BUSY_RECOVERY and SQLITE_BUSY_TIMEOUT. SQLITE_BUSY_SNAPSHOT
      # (517) stays Sequel's SerializationFailure: it isn't a wait that timed out.
      BUSY_CODES = [5, 261, 773].freeze

      # Extend +db+ when it's an SQLite database not already extended.
      # @param db [Sequel::Database]
      # @return [Sequel::Database] +db+
      def self.apply(db)
        db.extend(self) if db.adapter_scheme == :sqlite && !db.is_a?(self)
        db
      end

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
        BUSY_CODES.include?(sqlite_error_code(exception)) ? BusyError : super
      end
    end
  end
end
