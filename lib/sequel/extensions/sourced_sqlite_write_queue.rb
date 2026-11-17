# frozen_string_literal: true

require 'sourced'

module Sequel
  # Queues an SQLite database's writes in the process, so one fiber or thread
  # at a time waits for SQLite's single-writer lock, against other processes.
  #
  # SQLite's lock keeps no queue: a connection that finds it held retries,
  # and with {SourcedSQLiteBusyWait} a waiting fiber sleeps between retries,
  # while the process's other fibers take the lock whenever it's free, ahead
  # of it, until its wait times out. With this extension, transactions and
  # statements outside one (+execute_dui+, +execute_insert+, +execute_ddl+)
  # first take the database's turn, waiting for it in order. Whoever holds it
  # (+Sequel.current+, the owner of its connection: a fiber with Sequel's
  # +fiber_concurrency+ extension, which Sourced::Store loads, else a thread)
  # goes straight on, for a nested transaction or a statement in its
  # transaction. Reads outside a transaction don't queue.
  #
  # Waiting for the turn times out like waiting for SQLite's lock: after the
  # +:write_queue_timeout+ option, else +:timeout+ (Sequel's default,
  # 5000ms), raising {Sourced::Store::BusyError}. So a transaction that holds
  # the turn for long (ex. slow work in it) fails the process's other writes
  # as a transient error, as SQLite's lock would, rather than holding them
  # indefinitely.
  #
  # Everything written through the database queues, the application's own
  # writes included: a long read transaction, or work that shouldn't wait
  # behind the database's other writes, takes a database object of its own.
  #
  # @example
  #   db = Sequel.sqlite('app.db')
  #   db.extension(:sourced_sqlite_busy_wait, :sourced_sqlite_write_queue)
  module SourcedSQLiteWriteQueue
    DEFAULT_TIMEOUT = 5000

    def self.extended(db)
      db.instance_exec do
        @write_turn = Thread::Queue.new
        @write_turn << true
        @write_turn_holder = nil
        @write_turn_timeout = typecast_value_integer(opts[:write_queue_timeout] || opts.fetch(:timeout, DEFAULT_TIMEOUT)).fdiv(1000)
      end
    end

    def transaction(opts = OPTS, &block)
      in_write_turn { super }
    end

    def execute_dui(sql, opts = OPTS, &block)
      in_write_turn { super }
    end

    def execute_insert(sql, opts = OPTS, &block)
      in_write_turn { super }
    end

    def execute_ddl(sql, opts = OPTS, &block)
      in_write_turn { super }
    end

    private

    def in_write_turn
      return yield if @write_turn_holder.equal?(Sequel.current)

      unless @write_turn.pop(timeout: @write_turn_timeout)
        raise Sourced::Store::BusyError,
              "timed out after #{@write_turn_timeout}s waiting for another write in this process"
      end

      begin
        @write_turn_holder = Sequel.current
        yield
      ensure
        @write_turn_holder = nil
        @write_turn << true
      end
    end
  end

  Database.register_extension(:sourced_sqlite_write_queue, SourcedSQLiteWriteQueue)
end
