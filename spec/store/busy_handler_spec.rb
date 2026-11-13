# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'
require 'tmpdir'
require 'timeout'

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
    it 'extends an SQLite database once' do
      db = sqlite(timeout: 100)
      expect(described_class.apply(db)).to be(db)
      described_class.apply(db)

      expect(db.singleton_class.ancestors.count(described_class)).to eq(1)
    end

    it 'leaves other databases alone' do
      db = Sequel.mock
      described_class.apply(db)
      expect(db).not_to be_a(described_class)
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
