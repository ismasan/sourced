# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'
require 'tmpdir'
require 'timeout'

# Connections per fiber, as Sourced::Store has them
Sequel.extension(:fiber_concurrency)

RSpec.describe Sourced::Store::BusyHandler do
  around do |example|
    Dir.mktmpdir do |dir|
      @path = File.join(dir, 'busy.db')
      example.run
    end
  end

  def sqlite(timeout:) = Sequel.sqlite(@path, timeout:)

  # Holds SQLite's write lock on another connection, in a thread of the spec's
  # (standing in for another process), until the block releases it.
  def holding_write_lock(work: -> {})
    holder = sqlite(timeout: 5000)
    holder.create_table?(:items) { String :name }
    locked = Thread::Queue.new
    release = Thread::Queue.new
    thread = Thread.new do
      holder.transaction(mode: :immediate) do
        holder[:items].insert(name: 'held')
        locked << true
        work.call
        release.pop(timeout: 5)
      end
    end
    locked.pop
    yield release
  ensure
    release << true
    thread.join
    holder.disconnect
  end

  describe '.apply' do
    it 'applies the extensions to an SQLite database once' do
      db = sqlite(timeout: 100)
      expect(described_class.apply(db)).to be(db)
      described_class.apply(db)

      [Sequel::SourcedSQLiteBusyWait, Sequel::SourcedSQLiteWriteQueue].each do |mod|
        expect(db.singleton_class.ancestors.count(mod)).to eq(1)
      end
    end

    it 'leaves other databases alone' do
      db = Sequel.mock
      described_class.apply(db)
      expect(db).not_to be_a(Sequel::SourcedSQLiteBusyWait)
      expect(db).not_to be_a(Sequel::SourcedSQLiteWriteQueue)
    end
  end

  it 'is a pair of Sequel extensions, applied by name' do
    db = sqlite(timeout: 50).extension(:sourced_sqlite_busy_wait, :sourced_sqlite_write_queue)

    expect(db).to be_a(Sequel::SourcedSQLiteBusyWait)
    expect(db).to be_a(Sequel::SourcedSQLiteWriteQueue)
  end

  describe 'the write queue', :async do
    let(:db) { described_class.apply(sqlite(timeout: 50)) }

    before { db.create_table?(:items) { String :name } }

    def holding_turn(seconds)
      Async do
        db.transaction do
          db[:items].insert(name: 'holder')
          sleep seconds
        end
      end
    end

    it 'times out a write waiting for its turn, raising Store::BusyError' do
      holder = holding_turn(0.3)
      sleep 0.01

      expect { db[:items].insert(name: 'waiting') }.to raise_error(Sourced::Store::BusyError, /waiting for another write/) { |e|
        expect(e).to be_a(Sourced::StoreBusyError)
      }
      holder.wait
      expect(db[:items].select_map(:name)).to eq(%w[holder])
    end

    it "queues a write behind another fiber's transaction, waiting up to :write_queue_timeout over :timeout" do
      db = described_class.apply(Sequel.sqlite(@path, timeout: 50, write_queue_timeout: 1000))
      db.create_table?(:items) { String :name }
      holder = Async { db.transaction { db[:items].insert(name: 'holder'); sleep 0.2 } }
      sleep 0.01

      Timeout.timeout(2) { db[:items].insert(name: 'waited') }
      holder.wait
      expect(db[:items].select_map(:name)).to eq(%w[holder waited])
    end

    it "doesn't queue reads outside a transaction" do
      holder = holding_turn(0.3)
      sleep 0.01

      expect(Timeout.timeout(0.1) { db[:items].count }).to eq(0) # the holder's insert isn't committed
      holder.wait
    end

    it 'gives the turn back when the transaction raises' do
      expect { db.transaction { db[:items].insert(name: 'rolled back'); raise 'boom' } }.to raise_error('boom')

      Timeout.timeout(1) { db[:items].insert(name: 'next') }
      expect(db[:items].select_map(:name)).to eq(%w[next])
    end
  end

  it 'raises Store::BusyError, a transient store error, when the wait times out' do
    db = described_class.apply(sqlite(timeout: 50))

    holding_write_lock do
      expect { db[:items].insert(name: 'waiting') }.to raise_error(Sourced::Store::BusyError) { |e|
        expect(e).to be_a(Sourced::StoreBusyError)
        expect(e).to be_a(Sequel::DatabaseLockTimeout)
      }
    end
  end

  it 'runs nested transactions and their statements in the fiber holding the queue', :async do
    db = described_class.apply(sqlite(timeout: 50))
    db.create_table?(:items) { String :name }

    Timeout.timeout(2) do
      db.transaction do
        db[:items].insert(name: 'outer')
        db.transaction(savepoint: true) { db[:items].insert(name: 'inner') }
      end
    end

    expect(db[:items].select_map(:name)).to eq(%w[outer inner])
  end

  it "lets other fibers run while a fiber waits for another connection's lock, including connections opened before it was applied", :async do
    db = sqlite(timeout: 3000)
    db.test_connection
    described_class.apply(db)

    # The lock is held by another connection, as another process would.
    holding_write_lock do |release|
      ticks = 0
      ticker = Async { loop { ticks += 1; sleep 0.01 } }
      waiter = Async { db[:items].insert(name: 'waited') }

      sleep 0.2
      release << true
      Timeout.timeout(2) { waiter.wait }
      ticker.stop

      expect(ticks).to be >= 10
    end
    expect(db[:items].select_map(:name)).to contain_exactly('held', 'waited')
  end
end
