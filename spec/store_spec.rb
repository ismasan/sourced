# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/store'
require 'sequel'
require 'tmpdir'

# Define test messages for store specs (namespaced to avoid collisions)
module StoreTestMessages
  DeviceRegistered = Sourced::Message.define('store_test.device.registered') do
    attribute :device_id, String
    attribute :name, String
  end

  AssetRegistered = Sourced::Message.define('store_test.asset.registered') do
    attribute :asset_id, String
    attribute :label, String
  end

  DeviceBound = Sourced::Message.define('store_test.device.bound') do
    attribute :device_id, String
    attribute :asset_id, String
  end

  # Course/user messages for composite partition tests
  CourseCreated = Sourced::Message.define('store_test.course.created') do
    attribute :course_name, String
  end

  UserRegistered = Sourced::Message.define('store_test.user.registered') do
    attribute :user_id, String
    attribute :name, String
  end

  UserJoinedCourse = Sourced::Message.define('store_test.user.joined_course') do
    attribute :course_name, String
    attribute :user_id, String
  end

  CourseClosed = Sourced::Message.define('store_test.course.closed') do
    attribute :course_name, String
  end
end

RSpec.describe Sourced::Store do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::Store.new(db) }

  before do
    store.install!
  end

  describe '#installed?' do
    it 'returns true after install!' do
      expect(store.installed?).to be true
    end

    it 'returns false before install!' do
      fresh_db = Sequel.sqlite
      fresh_store = Sourced::Store.new(fresh_db)
      expect(fresh_store.installed?).to be false
    end
  end

  describe '#install!' do
    it 'creates the tables' do
      expect(db.table_exists?(:sourced_messages)).to be true
      expect(db.table_exists?(:sourced_key_pairs)).to be true
      expect(db.table_exists?(:sourced_message_key_pairs)).to be true
      expect(db.table_exists?(:sourced_scheduled_messages)).to be true
      expect(db.table_exists?(:sourced_consumer_groups)).to be true
      expect(db.table_exists?(:sourced_offsets)).to be true
      expect(db.table_exists?(:sourced_offset_key_pairs)).to be true
      expect(db.table_exists?(:sourced_workers)).to be true
    end

    it 'creates the claim-scan invalidation triggers' do
      triggers = db[:sqlite_master].where(type: 'trigger').select_map(:name)
      expect(triggers).to include('trg_sourced_ccc_dirty_on_message', 'trg_sourced_ccc_dirty_on_release')
    end

    it 'is idempotent' do
      expect { store.install! }.not_to raise_error
    end
  end

  describe '#append' do
    it 'appends a single message and returns position' do
      msg = StoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      )
      pos = store.append(msg)
      expect(pos).to eq(1)
    end

    it 'appends multiple messages and returns last position' do
      msgs = [
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }),
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' })
      ]
      pos = store.append(msgs)
      expect(pos).to eq(2)
    end

    it 'extracts and indexes key pairs (plus the always-present __id key)' do
      msg = StoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      )
      store.append(msg)

      # __id is always indexed so id-partitioned reactors can claim the message
      # regardless of which process appended it (see Store#effective_keys).
      key_pairs = db[:sourced_key_pairs].all
      expect(key_pairs.map { |r| [r[:name], r[:value]] }).to contain_exactly(
        ['__id', msg.id],
        ['device_id', 'dev-1'],
        ['name', 'Sensor A']
      )

      join_rows = db[:sourced_message_key_pairs].all
      expect(join_rows.size).to eq(3)
    end

    it 'deduplicates key pairs across messages' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      msg2 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor B' })
      store.append([msg1, msg2])

      # 'device_id'/'dev-1' should exist once in key_pairs
      count = db[:sourced_key_pairs].where(name: 'device_id', value: 'dev-1').count
      expect(count).to eq(1)

      # But both messages reference it via the join table
      kp_id = db[:sourced_key_pairs].where(name: 'device_id', value: 'dev-1').get(:id)
      join_count = db[:sourced_message_key_pairs].where(key_pair_id: kp_id).count
      expect(join_count).to eq(2)
    end

    it 'stores metadata as JSON' do
      msg = StoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { user_id: 42 }
      )
      store.append(msg)

      row = db[:sourced_messages].first
      meta = JSON.parse(row[:metadata], symbolize_names: true)
      expect(meta[:user_id]).to eq(42)
    end

    it 'persists and round-trips causation_id and correlation_id' do
      source = StoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      )
      caused = source.correlate(
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' })
      )
      store.append([source, caused])

      cond1 = Sourced::QueryCondition.new(message_type: 'store_test.device.registered', attrs: { device_id: 'dev-1' })
      cond2 = Sourced::QueryCondition.new(message_type: 'store_test.asset.registered', attrs: { asset_id: 'asset-1' })
      messages, = store.read([cond1, cond2])

      src = messages.find { |m| m.type == 'store_test.device.registered' }
      csd = messages.find { |m| m.type == 'store_test.asset.registered' }

      expect(src.causation_id).to eq(src.id)
      expect(src.correlation_id).to eq(src.id)
      expect(csd.causation_id).to eq(source.id)
      expect(csd.correlation_id).to eq(source.correlation_id)
    end

    it 'returns latest_position for empty array' do
      pos = store.append([])
      expect(pos).to eq(0)
    end
  end

  describe '#append scheduling and #update_schedule!' do
    it 'defers future-dated messages outside the main log until due' do
      now = Time.now
      delayed = StoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' }
      ).at(now + 60)

      store.append(delayed)

      expect(store.latest_position).to eq(0)
      expect(db[:sourced_scheduled_messages].count).to eq(1)
      expect(store.update_schedule!).to eq(0)
    end

    it 'appends past/now-dated messages immediately (no scheduling)' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      expect(store.latest_position).to eq(1)
      expect(db[:sourced_scheduled_messages].count).to eq(0)
    end

    it 'splits a mixed batch: immediate ones to the log, future ones to the schedule' do
      now = Time.now
      immediate = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      future = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' }).at(now + 60)

      store.append([immediate, future])

      expect(store.latest_position).to eq(1)
      expect(db[:sourced_messages].count).to eq(1)
      expect(db[:sourced_scheduled_messages].count).to eq(1)
    end

    it 'promotes due messages into the flat log and preserves metadata' do
      now = Time.now
      due = StoreTestMessages::DeviceRegistered.new(
        payload: { device_id: 'dev-1', name: 'Sensor A' },
        metadata: { source: 'test' }
      ).at(now + 2)

      store.append(due)

      Timecop.freeze(now + 3) do
        expect(store.update_schedule!).to eq(1)
      end

      expect(db[:sourced_scheduled_messages].count).to eq(0)
      expect(store.latest_position).to eq(1)

      cond = Sourced::QueryCondition.new(
        message_type: due.type,
        attrs: { device_id: 'dev-1' }
      )
      result = store.read([cond])
      msg = result.messages.first

      expect(msg).to be_a(StoreTestMessages::DeviceRegistered)
      expect(msg.created_at).to be >= Time.at((now + 3).to_i)
      expect(msg.metadata[:source]).to eq('test')
      expect(Time.parse(msg.metadata[:scheduled_at])).to be_a(Time)
    end

    it 'rolls back scheduling when a guard conflict aborts the append' do
      now = Time.now
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))
      _events, guard = store.read([cond])

      # Concurrent write invalidates the guard.
      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A v2' }))

      immediate = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a1' })
      future = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a2' }).at(now + 60)

      expect {
        store.append([immediate, future], guard: guard)
      }.to raise_error(Sourced::ConcurrentAppendError)

      # The whole call is atomic: the future message was NOT scheduled.
      expect(db[:sourced_scheduled_messages].count).to eq(0)
    end
  end

  describe '#append with guard (conditional append)' do
    let(:cond) do
      Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
    end

    it 'succeeds when no conflicts exist' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      _events, guard = store.read([cond])

      msg2 = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      pos = store.append(msg2, guard: guard)
      expect(pos).to eq(2)
    end

    it 'raises ConcurrentAppendError when conflicts exist' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      _events, guard = store.read([cond])

      # Concurrent write by another process
      conflicting = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A v2' })
      store.append(conflicting)

      new_msg = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect {
        store.append(new_msg, guard: guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'is atomic — failed append does not change store state' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      _events, guard = store.read([cond])

      # Concurrent write
      conflicting = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A v2' })
      store.append(conflicting)

      position_before = store.latest_position
      count_before = db[:sourced_messages].count

      new_msg = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect {
        store.append(new_msg, guard: guard)
      }.to raise_error(Sourced::ConcurrentAppendError)

      expect(store.latest_position).to eq(position_before)
      expect(db[:sourced_messages].count).to eq(count_before)
    end

    it 'works with a manually constructed guard' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      guard = Sourced::ConsistencyGuard.new(conditions: [cond], last_position: store.latest_position)

      msg2 = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      pos = store.append(msg2, guard: guard)
      expect(pos).to eq(2)
    end

    it 'append without guard still works unconditionally' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)

      msg2 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A v2' })
      pos = store.append(msg2)
      expect(pos).to eq(2)
    end
  end

  describe '#append concurrency (WAL BUSY_SNAPSHOT)', skip: (Process.respond_to?(:fork) ? false : 'requires fork') do
    # A guarded #append runs inside a transaction that READS (the guard's
    # conflict check) and then WRITES (the inserts). Under WAL, if that
    # transaction begins DEFERRED, its read pins a snapshot; if another
    # connection commits before the write, SQLite refuses to upgrade the
    # stale snapshot and raises SQLITE_BUSY_SNAPSHOT (a Sequel::SerializationFailure
    # NOT covered by busy_timeout). Beginning the write transaction IMMEDIATE
    # takes the write lock up front, so the concurrent writer waits instead.
    #
    # The concurrent writer runs in a *separate process* (via fork), matching the
    # real scenario: a second script appending while workers process. It must be
    # a separate process rather than a thread — the sqlite3 gem holds the GVL
    # while waiting out busy_timeout, so an in-process waiter would starve the
    # very thread that needs to run to release the lock.
    it 'does not raise BUSY_SNAPSHOT when a separate process commits mid-transaction' do
      Dir.mktmpdir do |dir|
        db_path = File.join(dir, 'busy_snapshot.db')

        store = Sourced::Store.new(Sequel.sqlite(db_path))
        store.install!

        # Seed a message so the guard has a real partition condition to read.
        store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }))
        _events, guard = store.read([
          Sourced::QueryCondition.new(
            message_type: 'store_test.device.registered',
            attrs: { device_id: 'dev-1' }
          )
        ])

        go_r, go_w     = IO.pipe # parent -> child: snapshot taken, you may commit
        done_r, done_w = IO.pipe # child -> parent: I have committed

        # Never share a SQLite handle across fork. Empty the pool before forking;
        # both parent and child reconnect lazily on next use. Other SQLite handles
        # elsewhere in the suite are still open at fork time — the sqlite3 gem
        # safely auto-closes the child's inherited copies; silence its warning.
        require 'sqlite3'
        SQLite3::ForkSafety.suppress_warnings! if defined?(SQLite3::ForkSafety)
        store.db.disconnect

        child = fork do
          go_w.close; done_r.close
          go_r.read(1) # wait until the parent has pinned its read snapshot
          child_store = Sourced::Store.new(Sequel.sqlite(db_path))
          child_store.append(StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-9', label: 'X' }))
          done_w.write('x')
          exit!(0) # skip at_exit / RSpec teardown in the child
        end
        go_r.close; done_w.close

        # After the parent's guard read pins the snapshot, release the child and
        # wait (bounded) for it to commit. Without the fix, the child (holding no
        # contended lock) commits immediately and the parent's following write
        # hits BUSY_SNAPSHOT. With the fix, the parent already holds the write
        # lock, so the child blocks; the wait times out and the parent commits
        # first — the child commits afterwards.
        allow(store).to receive(:check_conflicts).and_wrap_original do |orig, *args|
          result = orig.call(*args)
          go_w.write('x'); go_w.close
          IO.select([done_r], nil, nil, 2)
          result
        end

        appended = nil
        error = nil
        begin
          appended = store.append(
            StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a-9' }),
            guard: guard
          )
        rescue StandardError => e
          error = e
        end
        Process.wait(child)

        expect(error).to be_nil, "expected no BUSY_SNAPSHOT, got #{error&.class}: #{error&.message}"
        expect(appended).to be_a(Integer)
      end
    end
  end

  describe '#read_all' do
    it 'returns a ReadAllResult with messages and last_position' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-1', label: 'X' }),
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a-1' })
      ])

      result = store.read_all
      expect(result).to be_a(Sourced::ReadAllResult)
      expect(result.messages.size).to eq(3)
      expect(result.messages.map(&:position)).to eq([1, 2, 3])
      expect(result.last_position).to eq(3)
    end

    it 'supports #each to iterate current page messages' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-1', label: 'X' })
      ])

      result = store.read_all
      positions = result.map(&:position)
      expect(positions).to eq([1, 2])
    end

    it '#each without a block returns an enumerator for chaining' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-1', label: 'X' })
      ])

      result = store.read_all
      pairs = result.each.with_index.map { |msg, i| [i, msg.position] }
      expect(pairs).to eq([[0, 1], [1, 2]])
    end

    it 'supports destructuring into messages and last_position' do
      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

      messages, last_position = store.read_all
      expect(messages.size).to eq(1)
      expect(last_position).to eq(1)
    end

    it 'paginates with from_position and limit' do
      5.times do |i|
        store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: "dev-#{i}", name: "D#{i}" }))
      end

      result1 = store.read_all(limit: 2)
      expect(result1.messages.map(&:position)).to eq([1, 2])
      expect(result1.last_position).to eq(5)

      # from_position is inclusive, so position 2 is included
      result2 = store.read_all(from_position: 2, limit: 2)
      expect(result2.messages.map(&:position)).to eq([2, 3])

      result3 = store.read_all(from_position: 4, limit: 2)
      expect(result3.messages.map(&:position)).to eq([4, 5])
    end

    it 'returns empty messages with last_position 0 for an empty store' do
      result = store.read_all
      expect(result.messages).to eq([])
      expect(result.last_position).to eq(0)
    end

    it 'returns PositionedMessage instances' do
      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

      result = store.read_all
      expect(result.messages.first).to be_a(Sourced::PositionedMessage)
      expect(result.messages.first).to be_a(StoreTestMessages::DeviceRegistered)
    end

    describe 'PositionedMessage#to_message' do
      it 'unwraps to the underlying Sourced::Message instance' do
        store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

        wrapped = store.read_all.messages.first
        inner = wrapped.to_message

        expect(wrapped).to be_a(Sourced::PositionedMessage)
        expect(inner).to be_a(StoreTestMessages::DeviceRegistered)
        expect(inner).not_to be_a(Sourced::PositionedMessage)
        expect(inner.payload.device_id).to eq('dev-1')
      end

      it 'allows case/when against wrapped messages via Sourced::Message.===' do
        store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

        wrapped = store.read_all.messages.first
        matched = case wrapped
                  when StoreTestMessages::DeviceRegistered then :device
                  else :other
                  end
        expect(matched).to eq(:device)
      end
    end

    context 'with order: :desc' do
      before do
        5.times do |i|
          store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: "dev-#{i}", name: "D#{i}" }))
        end
      end

      it 'returns messages in descending position order' do
        result = store.read_all(order: :desc)
        expect(result.messages.map(&:position)).to eq([5, 4, 3, 2, 1])
        expect(result.last_position).to eq(5)
      end

      it 'paginates in descending order using from_position (inclusive)' do
        result1 = store.read_all(order: :desc, limit: 2)
        expect(result1.messages.map(&:position)).to eq([5, 4])

        # from_position is inclusive, so position 4 is included
        result2 = store.read_all(from_position: 4, order: :desc, limit: 2)
        expect(result2.messages.map(&:position)).to eq([4, 3])

        result3 = store.read_all(from_position: 2, order: :desc, limit: 2)
        expect(result3.messages.map(&:position)).to eq([2, 1])
      end

      it 'returns only the first message when from_position is 1 (inclusive)' do
        result = store.read_all(from_position: 1, order: :desc)
        expect(result.messages.map(&:position)).to eq([1])
        expect(result.last_position).to eq(5)
      end
    end

    describe '#to_enum' do
      before do
        5.times do |i|
          store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: "dev-#{i}", name: "D#{i}" }))
        end
      end

      it 'iterates all messages across pages in ascending order' do
        result = store.read_all(limit: 2)
        positions = result.to_enum.map(&:position)
        expect(positions).to eq([1, 2, 3, 4, 5])
      end

      it 'iterates all messages across pages in descending order' do
        result = store.read_all(order: :desc, limit: 2)
        positions = result.to_enum.map(&:position)
        expect(positions).to eq([5, 4, 3, 2, 1])
      end

      it 'works when all messages fit in a single page' do
        result = store.read_all(limit: 100)
        positions = result.to_enum.map(&:position)
        expect(positions).to eq([1, 2, 3, 4, 5])
      end

      it 'returns an empty enumerator for an empty store' do
        store.clear!
        result = store.read_all(limit: 2)
        expect(result.to_enum.to_a).to eq([])
      end

      it 'supports lazy enumeration' do
        result = store.read_all(limit: 2)
        first_three = result.to_enum.lazy.take(3).map(&:position).to_a
        expect(first_three).to eq([1, 2, 3])
      end
    end

    context 'with conditions' do
      before do
        store.append([
          StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }),
          StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' }),
          StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' }),
          StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'Sensor B' })
        ])
      end

      it 'filters messages matching conditions' do
        cond = Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-1' }
        )
        result = store.read_all(conditions: [cond])
        expect(result.messages.size).to eq(1)
        expect(result.messages.first.payload.device_id).to eq('dev-1')
      end

      it 'ORs multiple conditions' do
        cond1 = Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-1' }
        )
        cond2 = Sourced::QueryCondition.new(
          message_type: 'store_test.asset.registered',
          attrs: { asset_id: 'asset-1' }
        )
        result = store.read_all(conditions: [cond1, cond2])
        types = result.messages.map(&:type)
        expect(types).to eq(['store_test.device.registered', 'store_test.asset.registered'])
      end

      it 'combines conditions with from_position' do
        cond = Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-2' }
        )
        # dev-2 registered is at position 4
        result = store.read_all(conditions: [cond], from_position: 4)
        expect(result.messages.size).to eq(1)
        expect(result.messages.first.payload.device_id).to eq('dev-2')

        # from_position past it returns nothing
        result = store.read_all(conditions: [cond], from_position: 5)
        expect(result.messages).to be_empty
      end

      it 'returns empty messages when conditions match nothing' do
        cond = Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'nonexistent' }
        )
        result = store.read_all(conditions: [cond])
        expect(result.messages).to be_empty
      end

      it 'paginates with to_enum' do
        cond1 = Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-1' }
        )
        cond2 = Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-2' }
        )
        # DeviceRegistered messages at positions 1 and 4
        result = store.read_all(conditions: [cond1, cond2], limit: 1)
        positions = result.to_enum.map(&:position)
        expect(positions).to eq([1, 4])
      end
    end
  end

  describe '#read' do
    before do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }),
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'asset-1', label: 'Truck' }),
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'Sensor B' })
      ])
    end

    it 'queries by message_type and key condition' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
      results, guard = store.read([cond])
      expect(results.size).to eq(1)
      expect(results.first.type).to eq('store_test.device.registered')
      expect(results.first.payload.device_id).to eq('dev-1')
    end

    it 'returns messages with position' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
      results, _guard = store.read([cond])
      expect(results.first.position).to eq(1)
    end

    it 'queries with multiple OR conditions' do
      conditions = [
        Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-1' }
        ),
        Sourced::QueryCondition.new(
          message_type: 'store_test.device.bound',
          attrs: { device_id: 'dev-1' }
        )
      ]
      results, _guard = store.read(conditions)
      expect(results.size).to eq(2)
      expect(results.map(&:type)).to contain_exactly(
        'store_test.device.registered',
        'store_test.device.bound'
      )
    end

    it 'filters with after_position' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
      # dev-1 registered is at position 1, so after_position: 1 should return nothing
      results, _guard = store.read([cond], after_position: 1)
      expect(results).to be_empty

      # after_position: 0 should return it
      results, _guard = store.read([cond], after_position: 0)
      expect(results.size).to eq(1)
    end

    it 'applies limit' do
      conditions = [
        Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-1' }
        ),
        Sourced::QueryCondition.new(
          message_type: 'store_test.device.bound',
          attrs: { device_id: 'dev-1' }
        )
      ]
      results, _guard = store.read(conditions, limit: 1)
      expect(results.size).to eq(1)
      expect(results.first.position).to eq(1) # first by position order
    end

    it 'returns empty for non-matching conditions' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'nonexistent' }
      )
      results, _guard = store.read([cond])
      expect(results).to be_empty
    end

    it 'returns empty for empty conditions' do
      results, _guard = store.read([])
      expect(results).to be_empty
    end

    it 'deserializes into correct message subclasses' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
      results, _guard = store.read([cond])
      msg = results.first
      expect(msg).to be_a(StoreTestMessages::DeviceRegistered)
      expect(msg.id).to match(/\A[0-9a-f-]{36}\z/)
      expect(msg.created_at).to be_a(Time)
    end

    it 'returns a ConsistencyGuard with correct conditions' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )
      _results, guard = store.read([cond])
      expect(guard).to be_a(Sourced::ConsistencyGuard)
      expect(guard.conditions).to eq([cond])
    end

    it 'guard last_position reflects the last result position' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-2' }
      )
      results, guard = store.read([cond])
      # dev-2 is at position 4
      expect(results.size).to eq(1)
      expect(guard.last_position).to eq(4)
    end

    it 'guard last_position falls back to latest_position when no results' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'nonexistent' }
      )
      _results, guard = store.read([cond])
      expect(guard.last_position).to eq(store.latest_position)
    end

    it 'guard last_position falls back to after_position when no results and after_position given' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'nonexistent' }
      )
      _results, guard = store.read([cond], after_position: 2)
      expect(guard.last_position).to eq(2)
    end
  end

  describe '#read with compound conditions (AND within, OR across)' do
    before do
      store.append([
        # Two DeviceBound events with different (device_id, asset_id) combinations
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' }),
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-2' }),
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-2', asset_id: 'asset-1' }),
        # A single-attr message sharing device_id
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' }),
      ])
    end

    it 'compound condition ANDs attrs — only matches messages with all specified key pairs' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.bound',
        attrs: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      results, _guard = store.read([cond])
      expect(results.size).to eq(1)
      expect(results.first.payload.device_id).to eq('dev-1')
      expect(results.first.payload.asset_id).to eq('asset-1')
    end

    it 'excludes messages that match only one attr of a compound condition' do
      # dev-1/asset-2 shares device_id but not asset_id
      # dev-2/asset-1 shares asset_id but not device_id
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.bound',
        attrs: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      results, _guard = store.read([cond])
      expect(results.size).to eq(1)
      pairs = results.map { |m| [m.payload.device_id, m.payload.asset_id] }
      expect(pairs).to eq([['dev-1', 'asset-1']])
    end

    it 'ORs across separate conditions — compound and single-attr mixed' do
      conditions = [
        # Compound: only dev-1/asset-1
        Sourced::QueryCondition.new(
          message_type: 'store_test.device.bound',
          attrs: { device_id: 'dev-1', asset_id: 'asset-1' }
        ),
        # Single-attr: dev-1 registered
        Sourced::QueryCondition.new(
          message_type: 'store_test.device.registered',
          attrs: { device_id: 'dev-1' }
        )
      ]
      results, _guard = store.read(conditions)
      expect(results.size).to eq(2)
      expect(results.map(&:type)).to contain_exactly(
        'store_test.device.bound',
        'store_test.device.registered'
      )
    end

    it 'guard with compound condition detects conflicts correctly' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.bound',
        attrs: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      _results, guard = store.read([cond])

      # Concurrent write: same device_id + asset_id combination
      conflicting = StoreTestMessages::DeviceBound.new(
        payload: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      store.append(conflicting)

      new_msg = StoreTestMessages::DeviceBound.new(
        payload: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      expect {
        store.append(new_msg, guard: guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'guard with compound condition does NOT flag writes to a different partition as conflicts' do
      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.bound',
        attrs: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      _results, guard = store.read([cond])

      # Write to a DIFFERENT partition (dev-1/asset-2) — should NOT conflict
      other_partition = StoreTestMessages::DeviceBound.new(
        payload: { device_id: 'dev-1', asset_id: 'asset-2' }
      )
      store.append(other_partition)

      new_msg = StoreTestMessages::DeviceBound.new(
        payload: { device_id: 'dev-1', asset_id: 'asset-1' }
      )
      expect {
        store.append(new_msg, guard: guard)
      }.not_to raise_error
    end
  end

  describe '#messages_since' do
    it 'returns messages after the given position' do
      msg1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg1)
      pos = store.latest_position

      msg2 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A Updated' })
      store.append(msg2)

      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )

      conflicts, guard = store.messages_since([cond], pos)
      expect(conflicts.size).to eq(1)
      expect(conflicts.first.payload.name).to eq('Sensor A Updated')
      expect(guard).to be_a(Sourced::ConsistencyGuard)
    end

    it 'returns empty when no new messages' do
      msg = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor A' })
      store.append(msg)
      pos = store.latest_position

      cond = Sourced::QueryCondition.new(
        message_type: 'store_test.device.registered',
        attrs: { device_id: 'dev-1' }
      )

      conflicts, _guard = store.messages_since([cond], pos)
      expect(conflicts).to be_empty
    end
  end

  describe '#latest_position' do
    it 'returns 0 for empty store' do
      expect(store.latest_position).to eq(0)
    end

    it 'returns max position after appends' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])
      expect(store.latest_position).to eq(2)
    end
  end

  describe '#clear!' do
    it 'deletes all data and resets position' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      expect(store.latest_position).to eq(1)

      store.clear!

      expect(store.latest_position).to eq(0)
      expect(db[:sourced_messages].count).to eq(0)
      expect(db[:sourced_key_pairs].count).to eq(0)
      expect(db[:sourced_message_key_pairs].count).to eq(0)
    end

    it 'resets autoincrement so next position starts from 1' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.clear!

      pos = store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      )
      expect(pos).to eq(1)
    end

    it 'clears consumer groups, offsets, and offset_key_pairs' do
      store.register_consumer_group('test-group')
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('test-group',
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1')

      store.clear!

      expect(db[:sourced_consumer_groups].count).to eq(0)
      expect(db[:sourced_offsets].count).to eq(0)
      expect(db[:sourced_offset_key_pairs].count).to eq(0)
    end
  end

  describe '#register_consumer_group' do
    it 'creates row with active status' do
      store.register_consumer_group('my-group')
      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      expect(row).not_to be_nil
      expect(row[:status]).to eq('active')
      expect(row[:created_at]).not_to be_nil
      expect(row[:updated_at]).not_to be_nil
    end

    it 'is idempotent' do
      store.register_consumer_group('my-group')
      expect { store.register_consumer_group('my-group') }.not_to raise_error
      expect(db[:sourced_consumer_groups].where(group_id: 'my-group').count).to eq(1)
    end

  end

  describe '#consumer_group_active?' do
    it 'returns true for active group' do
      store.register_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be true
    end

    it 'returns false for stopped group' do
      store.register_consumer_group('my-group')
      store.stop_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be false
    end

    it 'returns false for nonexistent group' do
      expect(store.consumer_group_active?('nope')).to be false
    end

    it 'accepts an object responding to #group_id' do
      store.register_consumer_group('my-group')
      reactor = double('reactor', group_id: 'my-group')
      expect(store.consumer_group_active?(reactor)).to be true
    end
  end

  describe '#stop/start_consumer_group' do
    it 'toggles status' do
      store.register_consumer_group('my-group')
      store.stop_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be false

      store.start_consumer_group('my-group')
      expect(store.consumer_group_active?('my-group')).to be true
    end

    it 'accepts an object responding to #group_id' do
      store.register_consumer_group('my-group')
      reactor = double('reactor', group_id: 'my-group')

      store.stop_consumer_group(reactor)
      expect(store.consumer_group_active?('my-group')).to be false

      store.start_consumer_group(reactor)
      expect(store.consumer_group_active?('my-group')).to be true
    end

    it 'start_consumer_group clears retry_at and error_context' do
      store.register_consumer_group('my-group')

      # Set retry_at and error_context via updating_consumer_group
      store.updating_consumer_group('my-group') do |group|
        group.retry(Time.now + 60, retry_count: 1)
      end

      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      expect(row[:retry_at]).not_to be_nil
      expect(row[:error_context]).not_to be_nil

      store.start_consumer_group('my-group')

      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      expect(row[:retry_at]).to be_nil
      expect(row[:error_context]).to be_nil
      expect(row[:status]).to eq('active')
    end
  end

  describe '#updating_consumer_group' do
    before do
      store.register_consumer_group('my-group')
    end

    it 'yields a GroupUpdater and persists error_context' do
      store.updating_consumer_group('my-group') do |group|
        expect(group).to be_a(Sourced::GroupUpdater)
        group.retry(Time.now + 30, retry_count: 1)
      end

      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      ctx = JSON.parse(row[:error_context], symbolize_names: true)
      expect(ctx[:retry_count]).to eq(1)
      expect(row[:retry_at]).not_to be_nil
    end

    it 'persists error_context across successive calls (retry_count increments)' do
      store.updating_consumer_group('my-group') do |group|
        group.retry(Time.now + 30, retry_count: 1)
      end

      store.updating_consumer_group('my-group') do |group|
        expect(group.error_context[:retry_count]).to eq(1)
        group.retry(Time.now + 60, retry_count: 2)
      end

      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      ctx = JSON.parse(row[:error_context], symbolize_names: true)
      expect(ctx[:retry_count]).to eq(2)
    end

    it 'raises ArgumentError for nonexistent group' do
      expect {
        store.updating_consumer_group('nonexistent') { |_| }
      }.to raise_error(ArgumentError, /nonexistent/)
    end

    it 'stop sets status to STOPPED and clears retry_at' do
      # First set a retry_at
      store.updating_consumer_group('my-group') do |group|
        group.retry(Time.now + 30, retry_count: 1)
      end

      store.updating_consumer_group('my-group') do |group|
        group.stop(message: 'operator requested shutdown')
      end

      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      expect(row[:status]).to eq('stopped')
      expect(row[:retry_at]).to be_nil
    end

    it 'fail sets status to FAILED and clears retry_at' do
      store.updating_consumer_group('my-group') do |group|
        group.retry(Time.now + 30, retry_count: 1)
      end

      err = RuntimeError.new('test error')
      store.updating_consumer_group('my-group') do |group|
        group.fail(exception: err)
      end

      row = db[:sourced_consumer_groups].where(group_id: 'my-group').first
      expect(row[:status]).to eq('failed')
      expect(row[:retry_at]).to be_nil
      ctx = JSON.parse(row[:error_context], symbolize_names: true)
      expect(ctx[:exception_class]).to eq('RuntimeError')
      expect(ctx[:exception_message]).to eq('test error')
    end
  end

  describe '#reset_consumer_group' do
    it 'deletes all offsets' do
      store.register_consumer_group('my-group')
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('my-group',
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1')

      expect(db[:sourced_offsets].count).to be > 0
      store.reset_consumer_group('my-group')
      expect(db[:sourced_offsets].count).to eq(0)
    end

    it 'accepts an object responding to #group_id' do
      store.register_consumer_group('my-group')
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('my-group',
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1')

      reactor = double('reactor', group_id: 'my-group')
      expect(db[:sourced_offsets].count).to be > 0
      store.reset_consumer_group(reactor)
      expect(db[:sourced_offsets].count).to eq(0)
    end

    it 'is a no-op for queue-mode groups (offsets kept)' do
      store.register_consumer_group('queue-group', exclusive: true)
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('queue-group',
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1')

      expect(db[:sourced_offsets].count).to be > 0
      store.reset_consumer_group('queue-group')
      expect(db[:sourced_offsets].count).to be > 0
    end
  end

  describe '#claim_next (single attribute partition)' do
    let(:group_id) { 'single-test' }
    let(:handled_types) { ['store_test.device.registered'] }

    before do
      store.register_consumer_group(group_id)
    end

    it 'lazily discovers and creates offsets for new partitions' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(db[:sourced_offsets].count).to be >= 1
    end

    it 'returns nil when no pending messages' do
      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'returns messages for next unclaimed partition with correct shape' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).not_to be_nil
      expect(result).to be_a(Sourced::ClaimResult)
      expect(result.offset_id).to be_a(Integer)
      expect(result.key_pair_ids).to be_a(Array)
      expect(result.partition_key).to eq('device_id:dev-1')
      expect(result.partition_value).to eq({ 'device_id' => 'dev-1' })
      expect(result.messages).to be_a(Array)
      expect(result.messages.size).to eq(1)
      expect(result.messages.first).to be_a(StoreTestMessages::DeviceRegistered)
      expect(result.messages.first.position).to eq(1)
      expect(result.guard).to be_a(Sourced::ConsistencyGuard)
    end

    it 'returns multiple pending messages for same partition' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result.messages.size).to eq(2)
    end

    it 'skips claimed partitions — second worker gets different partition' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-2')

      expect(r1.partition_key).not_to eq(r2.partition_key)
    end

    it 'returns nil when all partitions claimed' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-2')
      expect(result).to be_nil
    end

    it 'respects handled_types filter' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: ['store_test.asset.registered'], worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'only returns messages after last_position' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # Append another message for same partition
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A updated' })
      )

      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2.messages.size).to eq(1)
      expect(r2.messages.first.payload.name).to eq('A updated')
    end

    it 'returns nil for stopped consumer group' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.stop_consumer_group(group_id)

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'returns nil when retry_at is in the future' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      # Set retry_at to the future
      store.updating_consumer_group(group_id) do |group|
        group.retry(Time.now + 3600, retry_count: 1)
      end

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil
    end

    it 'returns claims when retry_at is in the past' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      # Set retry_at to the past
      store.updating_consumer_group(group_id) do |group|
        group.retry(Time.now - 1, retry_count: 1)
      end

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).not_to be_nil
    end

    it 'prioritizes partition with earliest pending message' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      )
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      # dev-2 was appended first (position 1), so it should be prioritized
      expect(result.partition_value).to eq({ 'device_id' => 'dev-2' })
    end

    it 'bootstraps newly appeared partitions on subsequent calls' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # New partition appears
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-3', name: 'C' })
      )

      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2).not_to be_nil
      expect(r2.partition_value).to eq({ 'device_id' => 'dev-3' })
    end

    it 'returns a guard with conditions only for attrs each type actually has' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      guard = result.guard

      expect(guard).to be_a(Sourced::ConsistencyGuard)
      expect(guard.last_position).to eq(result.messages.last.position)

      # 1 handled_type with device_id attr = 1 condition
      expect(guard.conditions.size).to eq(1)
      cond = guard.conditions.first
      expect(cond.message_type).to eq('store_test.device.registered')
      expect(cond.attrs).to eq({ device_id: 'dev-1' })
    end

    it 'guard can be used for optimistic concurrency on append' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')

      # No concurrent writes — append with guard succeeds
      new_event = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect { store.append(new_event, guard: result.guard) }.not_to raise_error

      store.ack(group_id, offset_id: result.offset_id, position: result.messages.last.position)
    end

    it 'guard detects concurrent conflicting writes' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')

      # Simulate concurrent write after claim
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Concurrent' })
      )

      new_event = StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      expect {
        store.append(new_event, guard: result.guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end

    it 'replaying is false when consumer group has never processed the partition' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result.replaying).to be false
    end

    it 'replaying is false for new messages after ack' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # New message arrives after ack
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      )

      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2.replaying).to be false
    end

    it 'replaying is true when offset is reset and messages are re-claimed' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      # Process and ack — highest_position advances to 2
      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # Reset offsets — highest_position stays at 2
      store.reset_consumer_group(group_id)

      # Re-claim same messages — replaying because positions <= highest_position
      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2.replaying).to be true
      expect(r2.messages.size).to eq(2)
    end

    it 'replaying transitions to false once consumer passes highest_position after reset' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      # Process and ack up to position 1
      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # Reset offsets
      store.reset_consumer_group(group_id)

      # Add a new message at position 2
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      )

      # Re-claim: both messages, but last position (2) > highest_position (1)
      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2.replaying).to be false
      expect(r2.messages.size).to eq(2)
    end
  end

  describe '#claim_next (lazy discovery)' do
    let(:group_id) { 'discovery-test' }
    let(:handled_types) { ['store_test.device.registered', 'store_test.device.bound'] }

    before do
      store.register_consumer_group(group_id)
    end

    it 'advances discovery_position after discovering partitions' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')

      cg = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(cg[:discovery_position]).to be >= 1
    end

    it 'does not re-scan earlier messages on subsequent discovery calls' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      # First claim discovers both partitions and claims one
      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # Second claim uses fast path (no discovery needed)
      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2).not_to be_nil
      expect(r2.partition_key).not_to eq(r1.partition_key)
    end

    it 'discovers new partitions added after initial discovery' do
      # First batch of messages
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # No more work
      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil

      # New partition arrives
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      )

      # Should discover and claim the new partition
      r2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(r2).not_to be_nil
      expect(r2.partition_value).to eq({ 'device_id' => 'dev-2' })
    end

    it 'resets discovery_position when consumer group is reset' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      cg = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(cg[:discovery_position]).to be > 0

      store.reset_consumer_group(group_id)

      cg = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(cg[:discovery_position]).to eq(0)
    end

    it 'a new reactor catches up on old messages without explicit bootstrap' do
      # Pre-populate with multiple partitions
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-3', name: 'C' })
      ])

      # New reactor with no prior offsets
      new_group = 'new-reactor'
      store.register_consumer_group(new_group)

      # Should discover and process all partitions
      claimed_partitions = []
      3.times do
        r = store.claim_next(new_group, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
        break unless r

        claimed_partitions << r.partition_key
        store.ack(new_group, offset_id: r.offset_id, position: r.messages.last.position)
      end

      expect(claimed_partitions.size).to eq(3)
      expect(claimed_partitions).to contain_exactly('device_id:dev-1', 'device_id:dev-2', 'device_id:dev-3')
    end

    it 'does not short-circuit remaining partitions after acking the one at max position' do
      # Regression: highest_position short-circuit skipped unprocessed partitions
      # when one partition's last message happened to be at types_max_pos.
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-3', name: 'C' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-4', name: 'D' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-5', name: 'E' })
      ])

      new_group = 'multi-catch-up'
      store.register_consumer_group(new_group)

      claimed_partitions = []
      10.times do
        r = store.claim_next(new_group, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
        break unless r

        claimed_partitions << r.partition_key
        store.ack(new_group, offset_id: r.offset_id, position: r.messages.last.position)
      end

      expect(claimed_partitions.size).to eq(5)
      expect(claimed_partitions).to contain_exactly(
        'device_id:dev-1', 'device_id:dev-2', 'device_id:dev-3',
        'device_id:dev-4', 'device_id:dev-5'
      )
    end

    it 'only discovers partitions matching handled_types' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-1', label: 'Truck' })
      ])

      # Only handles device events, not asset events
      result = store.claim_next(group_id, partition_by: 'device_id', handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      expect(result).not_to be_nil
      expect(result.partition_value).to eq({ 'device_id' => 'dev-1' })

      # No more work for device_id partitions
      store.ack(group_id, offset_id: result.offset_id, position: result.messages.last.position)
      result2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      expect(result2).to be_nil
    end
  end

  describe '#claim_next (composite partition — conditional AND fetch)' do
    let(:group_id) { 'composite-test' }
    let(:handled_types) do
      [
        'store_test.course.created',
        'store_test.user.registered',
        'store_test.user.joined_course',
        'store_test.course.closed'
      ]
    end

    before do
      store.register_consumer_group(group_id)
    end

    it 'bootstraps composite partitions (only messages with ALL attributes create partitions)' do
      # CourseCreated only has course_name — not enough for a (course_name, user_id) partition
      store.append(
        StoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' })
      )

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil

      # UserJoinedCourse has both — NOW a partition is created
      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      )

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      expect(result).not_to be_nil
      expect(result.partition_value).to eq({ 'course_name' => 'Algebra', 'user_id' => 'joe' })
    end

    it 'fetches messages with single partition attribute matching' do
      store.append([
        StoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        StoreTestMessages::UserRegistered.new(payload: { user_id: 'joe', name: 'Joe' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      types = result.messages.map(&:type)
      expect(types).to contain_exactly(
        'store_test.course.created',
        'store_test.user.registered',
        'store_test.user.joined_course'
      )
    end

    it 'different composite partitions can be claimed in parallel' do
      store.append([
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Physics', user_id: 'jake' })
      ])

      r1 = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      r2 = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-2')

      expect(r1).not_to be_nil
      expect(r2).not_to be_nil
      expect(r1.partition_key).not_to eq(r2.partition_key)
    end

    it 'excludes messages with ALL partition attributes that do not match ALL values' do
      store.append([
        StoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'jake' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')

      # The first partition claimed should be one of the two — let's check its messages
      if result.partition_value['user_id'] == 'joe'
        # Should include CourseCreated (1 attr, matches) and joe's join (2 attrs, both match)
        # Should NOT include jake's join (2 attrs, user_id doesn't match)
        user_ids = result.messages
          .select { |m| m.type == 'store_test.user.joined_course' }
          .map { |m| m.payload.user_id }
        expect(user_ids).to eq(['joe'])
      else
        user_ids = result.messages
          .select { |m| m.type == 'store_test.user.joined_course' }
          .map { |m| m.payload.user_id }
        expect(user_ids).to eq(['jake'])
      end
    end

    it 'messages with ALL partition attributes matching are not duplicated' do
      store.append([
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      # The join message matches both key_pairs but should appear only once
      expect(result.messages.size).to eq(1)
    end

    it 'excludes messages with partial attributes matching wrong value' do
      store.append([
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'History', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')

      # Whichever partition we get, the other course's join should be excluded
      courses = result.messages.map { |m| m.payload.course_name }
      expect(courses.uniq.size).to eq(1)
    end

    it 'returns guard with one condition per message type containing only matching attrs' do
      store.append([
        StoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      guard = result.guard

      expect(guard.last_position).to eq(result.messages.last.position)

      # Expected conditions (one per message type, compound attrs):
      #   CourseCreated     has course_name only                       → 1 condition with { course_name: 'Algebra' }
      #   UserRegistered    has user_id (+ name, not a partition attr) → 1 condition with { user_id: 'joe' }
      #   CourseClosed      has course_name only                       → 1 condition with { course_name: 'Algebra' }
      #   UserJoinedCourse  has course_name + user_id                  → 1 condition with { course_name: 'Algebra', user_id: 'joe' }
      # Total: 4 conditions
      expect(guard.conditions.size).to eq(4)

      # CourseCreated has only course_name
      course_created_cond = guard.conditions.find { |c| c.message_type == 'store_test.course.created' }
      expect(course_created_cond.attrs).to eq({ course_name: 'Algebra' })

      # UserRegistered has only user_id
      user_registered_cond = guard.conditions.find { |c| c.message_type == 'store_test.user.registered' }
      expect(user_registered_cond.attrs).to eq({ user_id: 'joe' })

      # UserJoinedCourse has both attrs in a single condition
      joined_cond = guard.conditions.find { |c| c.message_type == 'store_test.user.joined_course' }
      expect(joined_cond.attrs).to eq({ course_name: 'Algebra', user_id: 'joe' })
    end

    it 'guard detects concurrent writes in composite partition' do
      store.append([
        StoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }),
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      ])

      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')

      # Concurrent write: course closed while decider was processing
      store.append(
        StoreTestMessages::CourseClosed.new(payload: { course_name: 'Algebra' })
      )

      new_event = StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      expect {
        store.append(new_event, guard: result.guard)
      }.to raise_error(Sourced::ConcurrentAppendError)
    end
  end

  describe '#claim_next (composite partition — cross-partition false positives)' do
    let(:group_id) { 'cross-partition-test' }
    let(:handled_types) { ['store_test.user.joined_course'] }

    before do
      store.register_consumer_group(group_id)
    end

    it 'acked partition with shared key_pair does not block other partitions with pending work' do
      # Partition by (course_name, user_id).
      # Two partitions share course_name='Algebra' but differ on user_id.
      #
      # After both are processed, a new message arrives for jake.
      # joe's acked partition still has a lower last_position, and the OR
      # join in find_and_claim_partition matches jake's new message via the
      # shared course_name key_pair. joe gets claimed first (lower OR-based
      # next_position), fetch_partition_messages (AND semantics) finds nothing,
      # and claim_next returns nil — never reaching jake's partition.

      # Step 1: append and process both partitions
      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      )
      r1 = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'jake' })
      )
      r2 = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      store.ack(group_id, offset_id: r2.offset_id, position: r2.messages.last.position)

      # joe: last_position=1, jake: last_position=2. Both fully caught up.

      # Step 2: new message for jake only
      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'jake' })
      )

      # Step 3: claim_next should find jake (the partition with real work),
      # not return nil because joe's OR-match on course_name got in the way.
      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      expect(result).not_to be_nil
      expect(result.partition_value).to eq({ 'course_name' => 'Algebra', 'user_id' => 'jake' })
      expect(result.messages.size).to eq(1)
      expect(result.messages.first.payload.user_id).to eq('jake')
    end

    it 'processes all partitions when many share a key_pair and new messages arrive' do
      # Simulates the seat selection scenario: many (showing_id, seat_id)
      # partitions share the same showing_id. After processing all initial
      # messages, new messages for specific partitions should be claimable
      # even though other acked partitions share the showing_id key_pair.
      seats = %w[A1 A2 A3 A4 A5]

      # Append and process one message per seat
      seats.each do |seat|
        store.append(
          StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: seat })
        )
      end
      seats.size.times do
        result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
        expect(result).not_to be_nil
        store.ack(group_id, offset_id: result.offset_id, position: result.messages.last.position)
      end

      # All caught up. Now append new messages for A3 and A5 only.
      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'A3' })
      )
      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'A5' })
      )

      # Should claim A3 and A5, not be blocked by A1/A2/A4's stale OR matches
      claimed_users = []
      2.times do |i|
        result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
        expect(result).not_to be_nil, "Expected partition #{i + 1} of 2 to be claimable but got nil (claimed so far: #{claimed_users})"
        claimed_users << result.partition_value['user_id']
        store.ack(group_id, offset_id: result.offset_id, position: result.messages.last.position)
      end

      expect(claimed_users).to contain_exactly('A3', 'A5')

      # No more work
      result = store.claim_next(group_id, partition_by: ['course_name', 'user_id'], handled_types: handled_types, worker_id: 'w-1')
      expect(result).to be_nil
    end
  end

  describe '#ack' do
    let(:group_id) { 'ack-test' }

    before do
      store.register_consumer_group(group_id)
    end

    it 'advances offset and releases claim' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')

      store.ack(group_id, offset_id: result.offset_id, position: result.messages.last.position)

      offset = db[:sourced_offsets].where(id: result.offset_id).first
      expect(offset[:last_position]).to eq(result.messages.last.position)
      expect(offset[:claimed]).to eq(0)
      expect(offset[:claimed_at]).to be_nil
      expect(offset[:claimed_by]).to be_nil
    end

    it 'after ack, subsequent claim skips processed messages' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      r1 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      # No new messages — should return nil
      r2 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      expect(r2).to be_nil
    end
  end

  describe '#ack_and_delete' do
    let(:group_id) { 'queue-test' }
    let(:handled_types) { ['store_test.device.registered'] }

    before do
      store.register_consumer_group(group_id, partition_by: 'device_id', exclusive: true)
    end

    def claim
      store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
    end

    it 'deletes the processed messages and their key-pair links' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      result = claim
      positions = result.messages.map(&:position)
      store.ack_and_delete(group_id, offset_id: result.offset_id, positions: positions)

      expect(db[:sourced_messages].where(position: positions).count).to eq(0)
      expect(db[:sourced_message_key_pairs].where(message_position: positions).count).to eq(0)
    end

    it 'keeps shared key_pairs rows' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = claim
      store.ack_and_delete(group_id, offset_id: result.offset_id, positions: result.messages.map(&:position))

      expect(db[:sourced_key_pairs].where(name: 'device_id', value: 'dev-1').count).to eq(1)
    end

    it 'keeps the offset row, releases the claim, and advances last_position' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = claim
      last = result.messages.last.position
      store.ack_and_delete(group_id, offset_id: result.offset_id, positions: [last])

      offset = db[:sourced_offsets].where(id: result.offset_id).first
      expect(offset).not_to be_nil
      expect(offset[:last_position]).to eq(last)
      expect(offset[:claimed]).to eq(0)
      expect(offset[:claimed_at]).to be_nil
      expect(offset[:claimed_by]).to be_nil
    end

    it 'advances the consumer group highest_position' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = claim
      last = result.messages.last.position
      store.ack_and_delete(group_id, offset_id: result.offset_id, positions: [last])

      cg = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(cg[:highest_position]).to eq(last)
    end

    it 'a subsequent claim returns only undeleted messages, in order' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      r1 = claim
      store.ack_and_delete(group_id, offset_id: r1.offset_id, positions: r1.messages.map(&:position))

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'C' })
      ])
      r2 = claim
      expect(r2.messages.map { |m| m.payload.name }).to eq(%w[B C])
    end

    it 'preserves per-partition ordering across partitions' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A1' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B1' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A2' })
      ])

      # Claim and delete dev-1's batch
      dev1 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      expect(dev1.partition_value).to eq({ 'device_id' => 'dev-1' })
      expect(dev1.messages.map { |m| m.payload.name }).to eq(%w[A1 A2])
      store.ack_and_delete(group_id, offset_id: dev1.offset_id, positions: dev1.messages.map(&:position))

      # dev-2 still claimable and intact
      dev2 = store.claim_next(group_id, partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-2')
      expect(dev2.partition_value).to eq({ 'device_id' => 'dev-2' })
      expect(dev2.messages.map { |m| m.payload.name }).to eq(%w[B1])
    end

    it 'never reuses positions after deleting the highest (AUTOINCREMENT)' do
      first = store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = claim
      store.ack_and_delete(group_id, offset_id: result.offset_id, positions: [first])

      second = store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      )
      expect(second).to be > first
    end
  end

  describe '#release_drained_offsets' do
    let(:handled_types) { ['store_test.device.registered'] }

    it 'deletes drained queue offsets (unclaimed, no remaining messages)' do
      store.register_consumer_group('queue-group', partition_by: 'device_id', exclusive: true)
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = store.claim_next('queue-group', partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.ack_and_delete('queue-group', offset_id: result.offset_id, positions: result.messages.map(&:position))

      expect(db[:sourced_offsets].count).to eq(1)
      reaped = store.release_drained_offsets
      expect(reaped).to eq(1)
      expect(db[:sourced_offsets].count).to eq(0)
      expect(db[:sourced_offset_key_pairs].count).to eq(0)
    end

    it 'keeps queue offsets that still have pending messages' do
      store.register_consumer_group('queue-group', partition_by: 'device_id', exclusive: true)
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      store.claim_next('queue-group', partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.release('queue-group', offset_id: db[:sourced_offsets].first[:id])

      expect(store.release_drained_offsets).to eq(0)
      expect(db[:sourced_offsets].count).to eq(1)
    end

    it 'keeps a retain (non-deleting) offset whose partition still has messages' do
      store.register_consumer_group('log-group', partition_by: 'device_id')
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = store.claim_next('log-group', partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      # ack retains the message (event-log semantics) -> partition not drained
      store.ack('log-group', offset_id: result.offset_id, position: result.messages.last.position)

      expect(store.release_drained_offsets).to eq(0)
      expect(db[:sourced_offsets].count).to eq(1)
    end

    it 'reaps a drained offset for a non-exclusive (log) group that deleted its messages' do
      # Deletion is decoupled from exclusivity: a log-mode reactor may delete via
      # a :delete action. Its drained offset must still be reaped (no leak).
      store.register_consumer_group('log-group', partition_by: 'device_id')
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = store.claim_next('log-group', partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      store.delete_messages(result.messages.map(&:position))
      store.ack('log-group', offset_id: result.offset_id, position: result.messages.last.position)

      expect(store.release_drained_offsets).to eq(1)
      expect(db[:sourced_offsets].count).to eq(0)
    end

    it 'leaves claimed queue offsets alone' do
      store.register_consumer_group('queue-group', partition_by: 'device_id', exclusive: true)
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      result = store.claim_next('queue-group', partition_by: 'device_id', handled_types: handled_types, worker_id: 'w-1')
      # Delete the message but leave the claim held (simulating an in-flight worker)
      db[:sourced_messages].where(position: result.messages.map(&:position)).delete

      expect(store.release_drained_offsets).to eq(0)
      expect(db[:sourced_offsets].count).to eq(1)
    end
  end

  describe 'append(index_by: :id) and id-partitioning' do
    let(:handled_types) { ['store_test.device.registered'] }

    it 'indexes only the message id, not payload attributes' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        index_by: :id
      )
      names = db[:sourced_key_pairs].select_map(:name).uniq
      expect(names).to eq(['__id'])
    end

    it 'resolves index basis from the registered group (no explicit index_by needed)' do
      store.register_consumer_group('q', partition_by: ['__id'], exclusive: true, handled_types: handled_types)
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )
      expect(db[:sourced_key_pairs].select_map(:name).uniq).to eq(['__id'])
    end

    it 'claims a message appended by another process that never registered the id-partitioned group' do
      # A CLI/console dispatching into a running app is a *separate process*, so
      # it has its own Store with an empty @type_index_basis and appends via the
      # default (payload) basis. The reserved __id key must still be indexed, or
      # the id-partitioned reactor could never discover/claim the message.
      store.register_consumer_group('q', partition_by: ['__id'], exclusive: true, handled_types: handled_types)

      other_process = Sourced::Store.new(db) # never registered the group
      msg = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      other_process.append(msg)

      claim = store.claim_next('q', partition_by: ['__id'], handled_types: handled_types, worker_id: 'w1')
      expect(claim).not_to be_nil
      expect(claim.messages.map(&:id)).to eq([msg.id])
    end

    it 'indexes promoted scheduled messages by id for an id-partitioned type' do
      now = Time.now
      store.register_consumer_group('q', partition_by: ['__id'], exclusive: true, handled_types: handled_types)
      msg = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }).at(now + 60)
      store.append(msg) # deferred to scheduled_messages (future-dated)
      Timecop.freeze(now + 61) { store.update_schedule! } # promotes via append — must still index by id

      expect(db[:sourced_key_pairs].select_map(:name).uniq).to eq(['__id'])
    end

    it 'gives each message its own partition (one offset per message), claimed by disjoint workers' do
      store.register_consumer_group('q', partition_by: ['__id'], exclusive: true)
      m1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      m2 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      store.append(m1, index_by: :id)
      store.append(m2, index_by: :id)

      c1 = store.claim_next('q', partition_by: ['__id'], handled_types: handled_types, worker_id: 'w1')
      c2 = store.claim_next('q', partition_by: ['__id'], handled_types: handled_types, worker_id: 'w2')
      c3 = store.claim_next('q', partition_by: ['__id'], handled_types: handled_types, worker_id: 'w3')

      expect(c1.messages.size).to eq(1)
      expect(c2.messages.size).to eq(1)
      expect(c3).to be_nil
      expect([c1, c2].flat_map { |c| c.messages.map(&:id) }.sort).to eq([m1.id, m2.id].sort)
      expect(db[:sourced_offsets].count).to eq(2)
    end

    it 'delete + reap prunes the drained offset and the orphaned id key_pair' do
      store.register_consumer_group('q', partition_by: ['__id'], exclusive: true)
      m1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      m2 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      store.append(m1, index_by: :id)
      store.append(m2, index_by: :id)

      c1 = store.claim_next('q', partition_by: ['__id'], handled_types: handled_types, worker_id: 'w1')
      store.ack_and_delete('q', offset_id: c1.offset_id, positions: c1.messages.map(&:position))

      expect(db[:sourced_messages].count).to eq(1) # m2 remains

      expect(store.release_drained_offsets).to eq(1) # c1's drained offset
      expect(store.prune_orphan_key_pairs).to eq(1)      # c1's orphaned id key_pair
      expect(db[:sourced_key_pairs].where(name: '__id').count).to eq(1) # m2's id remains
    end
  end

  describe '#advance_offset' do
    let(:group_id) { 'advance-test' }
    let(:handled_types) { ['store_test.device.registered', 'store_test.device.bound'] }

    before do
      store.register_consumer_group(group_id)
    end

    it 'creates and advances offset for a new partition' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 1
      )

      offset = db[:sourced_offsets].join(:sourced_consumer_groups, id: :consumer_group_id)
        .where(Sequel[:sourced_consumer_groups][:group_id] => group_id)
        .first
      expect(offset[:last_position]).to eq(1)
    end

    it 'advances highest_position on the consumer group' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 1
      )

      cg = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(cg[:highest_position]).to eq(1)
    end

    it 'never decreases the offset' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
      ])

      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 2
      )

      # Try to go backwards
      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 1
      )

      offset = db[:sourced_offsets].join(:sourced_consumer_groups, id: :consumer_group_id)
        .where(Sequel[:sourced_consumer_groups][:group_id] => group_id)
        .first
      expect(offset[:last_position]).to eq(2)
    end

    it 'never decreases highest_position' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 2
      )

      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-2' },
        position: 1
      )

      cg = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(cg[:highest_position]).to eq(2)
    end

    it 'causes claim_next to skip the advanced partition' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 1
      )

      result = store.claim_next(group_id,
        partition_by: 'device_id',
        handled_types: handled_types,
        worker_id: 'w-1'
      )
      expect(result).to be_nil
    end

    it 'only skips messages up to the advanced position' do
      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a1' })
      ])

      # Advance past only the first message
      store.advance_offset(group_id,
        partition: { 'device_id' => 'dev-1' },
        position: 1
      )

      result = store.claim_next(group_id,
        partition_by: 'device_id',
        handled_types: handled_types,
        worker_id: 'w-1'
      )
      expect(result).not_to be_nil
      expect(result.messages.size).to eq(1)
      expect(result.messages.first.type).to eq('store_test.device.bound')
    end

    it 'is a no-op for nonexistent consumer group' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      expect {
        store.advance_offset('nonexistent',
          partition: { 'device_id' => 'dev-1' },
          position: 1
        )
      }.not_to raise_error

      expect(db[:sourced_offsets].count).to eq(0)
    end

    it 'is a no-op when partition has no messages in the store' do
      expect {
        store.advance_offset(group_id,
          partition: { 'device_id' => 'dev-1' },
          position: 1
        )
      }.not_to raise_error

      expect(db[:sourced_offsets].count).to eq(0)
    end

    it 'works with composite partitions' do
      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      )

      store.advance_offset(group_id,
        partition: { 'course_name' => 'Algebra', 'user_id' => 'joe' },
        position: 1
      )

      offset = db[:sourced_offsets].join(:sourced_consumer_groups, id: :consumer_group_id)
        .where(Sequel[:sourced_consumer_groups][:group_id] => group_id)
        .first
      expect(offset[:last_position]).to eq(1)
      expect(offset[:partition_key]).to eq('course_name:Algebra|user_id:joe')
    end
  end

  describe '#stats' do
    it 'returns zeroes for an empty store' do
      result = store.stats
      expect(result).to be_a(Sourced::Stats)
      expect(result.max_position).to eq(0)
      expect(result.groups).to eq([])
    end

    it 'returns groups with zeroed stats when no messages processed' do
      store.register_consumer_group('group-a')
      store.register_consumer_group('group-b')

      result = store.stats
      expect(result.max_position).to eq(0)
      expect(result.groups.size).to eq(2)

      group_a = result.groups.find { |g| g[:group_id] == 'group-a' }
      expect(group_a[:status]).to eq('active')
      expect(group_a[:retry_at]).to be_nil
      expect(group_a[:oldest_processed]).to eq(0)
      expect(group_a[:newest_processed]).to eq(0)
      expect(group_a[:partition_count]).to eq(0)
    end

    it 'reflects processing state after claim and ack' do
      group_id = 'stats-test'
      store.register_consumer_group(group_id)

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      # Claim and ack first partition
      r1 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)

      result = store.stats
      expect(result.max_position).to eq(2)

      group = result.groups.first
      expect(group[:group_id]).to eq(group_id)
      expect(group[:partition_count]).to eq(2) # both partitions bootstrapped
      expect(group[:oldest_processed]).to be > 0
      expect(group[:newest_processed]).to be > 0
    end

    it 'reports stopped and failed group statuses with error context' do
      store.register_consumer_group('active-group')
      store.register_consumer_group('stopped-group')
      store.register_consumer_group('failed-group')

      store.stop_consumer_group('stopped-group', 'maintenance')
      store.updating_consumer_group('failed-group') { |g| g.fail(exception: RuntimeError.new('boom')) }

      result = store.stats
      by_id = result.groups.map { |g| [g[:group_id], g] }.to_h

      expect(by_id['active-group'][:status]).to eq('active')
      expect(by_id['active-group'][:error_context]).to eq({})

      expect(by_id['stopped-group'][:status]).to eq('stopped')
      expect(by_id['stopped-group'][:error_context][:message]).to eq('maintenance')

      expect(by_id['failed-group'][:status]).to eq('failed')
      expect(by_id['failed-group'][:error_context][:exception_class]).to eq('RuntimeError')
      expect(by_id['failed-group'][:error_context][:exception_message]).to eq('boom')
    end

    it 'groups are ordered by group_id' do
      store.register_consumer_group('zebra')
      store.register_consumer_group('alpha')
      store.register_consumer_group('middle')

      result = store.stats
      expect(result.groups.map { |g| g[:group_id] }).to eq(%w[alpha middle zebra])
    end
  end

  describe '#read_offsets' do
    it 'returns empty result when no offsets exist' do
      result = store.read_offsets
      expect(result).to be_a(Sourced::OffsetsResult)
      expect(result.offsets).to eq([])
      expect(result.total_count).to eq(0)
    end

    it 'returns all offsets across groups with group_name and group_status' do
      store.register_consumer_group('group-a')
      store.register_consumer_group('group-b')

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      # Claim to bootstrap offsets in both groups
      store.claim_next('group-a', partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.claim_next('group-b', partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-2')

      result = store.read_offsets
      expect(result.total_count).to be >= 2

      group_names = result.offsets.map { |o| o[:group_name] }.uniq.sort
      expect(group_names).to include('group-a', 'group-b')

      result.offsets.each do |o|
        expect(o).to have_key(:id)
        expect(o).to have_key(:group_name)
        expect(o).to have_key(:group_status)
        expect(o).to have_key(:partition_key)
        expect(o).to have_key(:last_position)
        expect(o).to have_key(:claimed)
        expect(o).to have_key(:claimed_at)
        expect(o).to have_key(:claimed_by)
        expect(o[:group_status]).to eq('active')
      end
    end

    it 'filters by group_id when provided' do
      store.register_consumer_group('group-a')
      store.register_consumer_group('group-b')

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      ])

      store.claim_next('group-a', partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.claim_next('group-b', partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-2')

      result = store.read_offsets(group_id: 'group-a')
      expect(result.offsets).to all(satisfy { |o| o[:group_name] == 'group-a' })
      expect(result.total_count).to eq(result.offsets.size)
    end

    it 'paginates with from_id and limit (inclusive)' do
      store.register_consumer_group('group-a')

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-3', name: 'C' })
      ])

      # Claim each to bootstrap offsets
      3.times do
        store.claim_next('group-a', partition_by: 'device_id',
          handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      end

      # Get first page of 2
      page1 = store.read_offsets(group_id: 'group-a', limit: 2)
      expect(page1.offsets.size).to eq(2)
      expect(page1.total_count).to eq(3)

      # Get second page starting from next id (inclusive)
      page2 = store.read_offsets(group_id: 'group-a', limit: 2, from_id: page1.offsets.last[:id] + 1)
      expect(page2.offsets.size).to eq(1)

      # No overlap between pages
      page1_ids = page1.offsets.map { |o| o[:id] }
      page2_ids = page2.offsets.map { |o| o[:id] }
      expect(page1_ids & page2_ids).to be_empty
    end

    it 'to_enum iterates across pages' do
      store.register_consumer_group('group-a')

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-3', name: 'C' })
      ])

      3.times do
        store.claim_next('group-a', partition_by: 'device_id',
          handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      end

      # Paginate with limit: 1 to force multiple fetches
      result = store.read_offsets(group_id: 'group-a', limit: 1)
      all_offsets = result.to_enum.to_a
      expect(all_offsets.size).to eq(3)
      expect(all_offsets.map { |o| o[:id] }).to eq(all_offsets.map { |o| o[:id] }.sort)
    end

    it 'includes claim status fields' do
      store.register_consumer_group('group-a')

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      ])

      # Claim creates an offset that is claimed
      claim = store.claim_next('group-a', partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')

      result = store.read_offsets(group_id: 'group-a')
      offset = result.offsets.first

      expect(offset[:claimed]).to be true
      expect(offset[:claimed_by]).to eq('w-1')

      # Ack releases the claim
      store.ack('group-a', offset_id: claim.offset_id, position: claim.messages.last.position)

      result = store.read_offsets(group_id: 'group-a')
      offset = result.offsets.first

      expect(offset[:claimed]).to be false
    end

    it 'supports array destructuring' do
      offsets, total = store.read_offsets
      expect(offsets).to eq([])
      expect(total).to eq(0)
    end
  end

  describe '#read_correlation_batch' do
    it 'returns all messages sharing the same correlation_id, ordered by position' do
      # Create a command (source of the correlation chain)
      cmd = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'Sensor' })

      # Create correlated events
      evt1 = cmd.correlate(StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a-1' }))
      evt2 = cmd.correlate(StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-1', label: 'Asset A' }))

      store.append([cmd, evt1, evt2])

      results = store.read_correlation_batch(cmd.id)
      expect(results.size).to eq(3)
      expect(results.map(&:id)).to eq([cmd.id, evt1.id, evt2.id])
      expect(results.map(&:position)).to eq(results.map(&:position).sort)
    end

    it 'returns [] for an unknown message_id' do
      expect(store.read_correlation_batch(SecureRandom.uuid)).to eq([])
    end

    it 'excludes messages with a different correlation_id' do
      cmd1 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      evt1 = cmd1.correlate(StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a-1' }))

      # Unrelated chain
      cmd2 = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      evt2 = cmd2.correlate(StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-2', asset_id: 'a-2' }))

      store.append([cmd1, evt1, cmd2, evt2])

      results = store.read_correlation_batch(cmd1.id)
      expect(results.size).to eq(2)
      expect(results.map(&:id)).to eq([cmd1.id, evt1.id])
    end

    it 'can be queried from any message in the chain' do
      cmd = StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      evt = cmd.correlate(StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'a-1' }))

      store.append([cmd, evt])

      # Query from the event, not the command
      results = store.read_correlation_batch(evt.id)
      expect(results.size).to eq(2)
      expect(results.map(&:id)).to eq([cmd.id, evt.id])
    end
  end

  describe '#release' do
    let(:group_id) { 'release-test' }

    before do
      store.register_consumer_group(group_id)
    end

    it 'releases claim without advancing' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      result = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')

      store.release(group_id, offset_id: result.offset_id)

      offset = db[:sourced_offsets].where(id: result.offset_id).first
      expect(offset[:last_position]).to eq(0) # not advanced
      expect(offset[:claimed]).to eq(0)
    end

    it 'after release, same partition re-claimed with same messages' do
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      r1 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-1')
      store.release(group_id, offset_id: r1.offset_id)

      r2 = store.claim_next(group_id, partition_by: 'device_id',
        handled_types: ['store_test.device.registered'], worker_id: 'w-2')

      expect(r2.offset_id).to eq(r1.offset_id)
      expect(r2.messages.map(&:position)).to eq(r1.messages.map(&:position))
    end
  end

  describe 'eager offset creation' do
    let(:group_id) { 'eager-test-group' }

    it 'creates offsets during append when partition_by is registered' do
      store.register_consumer_group(group_id, partition_by: [:device_id])

      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      offsets = db[:sourced_offsets].all
      expect(offsets.size).to eq(1)
      expect(offsets.first[:partition_key]).to eq('device_id:dev-1')
    end

    it 'creates offsets for multiple partitions in a single append' do
      store.register_consumer_group(group_id, partition_by: [:device_id])

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-2', name: 'B' })
      ])

      offsets = db[:sourced_offsets].order(:partition_key).all
      expect(offsets.size).to eq(2)
      expect(offsets.map { |o| o[:partition_key] }).to eq(['device_id:dev-1', 'device_id:dev-2'])
    end

    it 'deduplicates offsets within the same append batch' do
      store.register_consumer_group(group_id, partition_by: [:device_id])

      store.append([
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
        StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' })
      ])

      offsets = db[:sourced_offsets].all
      expect(offsets.size).to eq(1)
    end

    it 'is idempotent across multiple appends' do
      store.register_consumer_group(group_id, partition_by: [:device_id])

      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))
      store.append(StoreTestMessages::DeviceBound.new(payload: { device_id: 'dev-1', asset_id: 'asset-1' }))

      offsets = db[:sourced_offsets].all
      expect(offsets.size).to eq(1)
    end

    it 'skips messages missing partition attributes' do
      store.register_consumer_group(group_id, partition_by: [:device_id])

      # AssetRegistered has no device_id
      store.append(StoreTestMessages::AssetRegistered.new(payload: { asset_id: 'a-1', label: 'X' }))

      offsets = db[:sourced_offsets].all
      expect(offsets).to be_empty
    end

    it 'creates offsets for composite partitions' do
      store.register_consumer_group(group_id, partition_by: [:course_name, :user_id])

      store.append(
        StoreTestMessages::UserJoinedCourse.new(payload: { course_name: 'Algebra', user_id: 'joe' })
      )

      offsets = db[:sourced_offsets].all
      expect(offsets.size).to eq(1)
      expect(offsets.first[:partition_key]).to eq('course_name:Algebra|user_id:joe')
    end

    it 'skips messages with only partial composite partition attributes' do
      store.register_consumer_group(group_id, partition_by: [:course_name, :user_id])

      # CourseCreated only has course_name, not user_id
      store.append(StoreTestMessages::CourseCreated.new(payload: { course_name: 'Algebra' }))

      offsets = db[:sourced_offsets].all
      expect(offsets).to be_empty
    end

    it 'does not create offsets when no consumer groups are registered' do
      # No register_consumer_group call
      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

      offsets = db[:sourced_offsets].all
      expect(offsets).to be_empty
    end

    it 'does not create offsets when partition_by is nil (legacy group)' do
      store.register_consumer_group(group_id) # no partition_by

      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

      offsets = db[:sourced_offsets].all
      expect(offsets).to be_empty
    end

    it 'creates offsets for multiple registered consumer groups' do
      store.register_consumer_group('group-a', partition_by: [:device_id])
      store.register_consumer_group('group-b', partition_by: [:device_id])

      store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

      cg_a = db[:sourced_consumer_groups].where(group_id: 'group-a').first
      cg_b = db[:sourced_consumer_groups].where(group_id: 'group-b').first

      offsets_a = db[:sourced_offsets].where(consumer_group_id: cg_a[:id]).all
      offsets_b = db[:sourced_offsets].where(consumer_group_id: cg_b[:id]).all

      expect(offsets_a.size).to eq(1)
      expect(offsets_b.size).to eq(1)
    end

    it 'persists partition_by in consumer_groups table' do
      store.register_consumer_group(group_id, partition_by: [:device_id, :name])

      row = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(JSON.parse(row[:partition_by])).to eq(['device_id', 'name'])
    end

    it 'updates partition_by on re-registration' do
      store.register_consumer_group(group_id, partition_by: [:device_id])
      store.register_consumer_group(group_id, partition_by: [:asset_id])

      row = db[:sourced_consumer_groups].where(group_id: group_id).first
      expect(JSON.parse(row[:partition_by])).to eq(['asset_id'])
    end

    it 'claim_next skips discovery when offsets already exist from append' do
      store.register_consumer_group(group_id, partition_by: [:device_id])

      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      # Offsets already exist from append — claim should work without discovery
      result = store.claim_next(
        group_id,
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1'
      )

      expect(result).not_to be_nil
      expect(result.messages.size).to eq(1)
      expect(result.partition_value).to eq({ 'device_id' => 'dev-1' })
    end

    it 'claim_next does not strand eager offsets when last_nil_types_max_pos is latched at/above their position' do
      # Regression: under a concurrent append burst a poll can record
      # last_nil_types_max_pos == the max handled-type position while eager
      # offsets for those very messages are still pending (their appends were
      # not yet visible to the polling worker's snapshot). Because a decider's
      # handled_types exclude its evolve types, processing those messages only
      # emits events — never a new command — so the watermark never rises again
      # and the short-circuit would strand the partitions forever.
      store.register_consumer_group(group_id, partition_by: [:device_id])

      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      # Simulate the latch: pin the watermark at the pending message's position.
      max_pos = db[:sourced_messages].max(:position)
      db[:sourced_consumer_groups]
        .where(group_id: group_id)
        .update(last_nil_types_max_pos: max_pos)

      result = store.claim_next(
        group_id,
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1'
      )

      # The eager path must ignore the watermark and claim the pending partition.
      expect(result).not_to be_nil
      expect(result.messages.size).to eq(1)
      expect(result.partition_value).to eq({ 'device_id' => 'dev-1' })
    end

    describe 'nil-scan gate (trigger-invalidated scan version)' do
      # For partitioned groups, last_nil_types_max_pos holds a sign-encoded scan
      # version: >= 0 dirty (scan), < 0 latched-clean at version -value (skip).
      # DB triggers bump it on message insert and on claim release.
      let(:handled) { ['store_test.device.registered'] }

      def claim(worker: 'w-1', batch_size: nil)
        store.claim_next(group_id, partition_by: 'device_id', handled_types: handled, worker_id: worker, batch_size: batch_size)
      end

      def gate_version
        db[:sourced_consumer_groups].where(group_id: group_id).get(:last_nil_types_max_pos)
      end

      before do
        store.register_consumer_group(group_id, partition_by: [:device_id])
      end

      it 'latches clean after an empty scan and short-circuits while nothing changes' do
        expect(claim).to be_nil
        version = gate_version
        expect(version).to be_negative

        # Second poll short-circuits on the latch: version untouched.
        expect(claim).to be_nil
        expect(gate_version).to eq(version)
      end

      it 'an append re-opens a latched gate (message trigger)' do
        expect(claim).to be_nil
        expect(gate_version).to be_negative

        store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))
        expect(gate_version).to be_positive

        result = claim
        expect(result).not_to be_nil
        expect(result.partition_value).to eq({ 'device_id' => 'dev-1' })
      end

      it 'a partial ack re-opens the gate so the batch remainder is claimable (release trigger)' do
        store.append([
          StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }),
          StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'B' })
        ])

        r1 = claim(batch_size: 1)
        expect(r1.messages.size).to eq(1)

        # Sibling polls while the partition is claimed: nothing claimable → latch.
        expect(claim(worker: 'w-2')).to be_nil
        expect(gate_version).to be_negative

        # Partial ack releases the claim; the trigger re-opens the gate so the
        # remainder isn't stranded behind the sibling's latch.
        store.ack(group_id, offset_id: r1.offset_id, position: r1.messages.last.position)
        r2 = claim(worker: 'w-2')
        expect(r2).not_to be_nil
        expect(r2.messages.map { |m| m.payload.name }).to eq(['B'])
      end

      it 'a release re-opens the gate (release trigger)' do
        store.append(StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' }))

        r1 = claim
        expect(r1).not_to be_nil

        expect(claim(worker: 'w-2')).to be_nil
        expect(gate_version).to be_negative

        store.release(group_id, offset_id: r1.offset_id)
        r2 = claim(worker: 'w-2')
        expect(r2).not_to be_nil
        expect(r2.messages.size).to eq(1)
      end

      it 'start_consumer_group marks the group dirty so it rescans on resume' do
        expect(claim).to be_nil
        expect(gate_version).to be_negative

        store.stop_consumer_group(group_id)
        store.start_consumer_group(group_id)
        expect(gate_version).to eq(0)
      end
    end

    it 'claim_next falls back to discovery for pre-existing messages' do
      # Append BEFORE registering — no eager offsets
      store.append(
        StoreTestMessages::DeviceRegistered.new(payload: { device_id: 'dev-1', name: 'A' })
      )

      store.register_consumer_group(group_id, partition_by: [:device_id])

      # claim_next should still find the message via discovery fallback
      result = store.claim_next(
        group_id,
        partition_by: 'device_id',
        handled_types: ['store_test.device.registered'],
        worker_id: 'w-1'
      )

      expect(result).not_to be_nil
      expect(result.messages.size).to eq(1)
    end
  end
end
