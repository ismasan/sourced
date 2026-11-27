# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'

module SnapshotSpec
  Add = Sourced::Command.define('snapshot_spec.add') do
    reference :counter_id, String
    attribute :amount, Integer
  end

  Added = Sourced::Event.define('snapshot_spec.added') do
    reference :counter_id, String
    attribute :amount, Integer
  end

  Noted = Sourced::Event.define('snapshot_spec.noted') do
    reference :counter_id, String
    attribute :total, Integer
  end

  Since = Sourced::Event.define('snapshot_spec.since') do
    reference :counter_id, String
    attribute :on, Date
  end

  Track = Sourced::Command.define('snapshot_spec.track') do
    reference :counter_id, String
    attribute :on, Date
  end

  class Counter < Sourced::Decider
    partition_by :counter_id
    consumer_group 'snapshot_spec.counter'
    snapshot every: 3, version: 1

    context { { total: 0, adds: 0 } }

    evolve Added do |context, evt|
      context[:total] += evt.payload.amount
      context[:adds] += 1
    end

    command Add => Added do |_context, cmd|
      raise ArgumentError, 'negative' if cmd.payload.amount.negative?

      event Added, counter_id: cmd.payload.counter_id, amount: cmd.payload.amount
    end
  end

  # Handles commands and reacts to the events it evolves, in one partition.
  class Reacting < Sourced::Decider
    partition_by :counter_id
    consumer_group 'snapshot_spec.reacting'
    snapshot every: 2, version: 1

    context { { total: 0, noted: 0 } }

    evolve(Added) { |context, evt| context[:total] += evt.payload.amount }
    evolve(Noted) { |context, _evt| context[:noted] += 1 }

    command Add => Added do |_context, cmd|
      event Added, counter_id: cmd.payload.counter_id, amount: cmd.payload.amount
    end

    reaction Added => Noted do |context, evt|
      dispatch Noted, counter_id: evt.payload.counter_id, total: context[:total]
    end
  end

  State = Sourced::Types::Hash[since: Sourced::Types::Date.nullable]

  class Typed < Sourced::Decider
    partition_by :counter_id
    consumer_group 'snapshot_spec.typed'
    snapshot every: 1, version: 1, type: State

    context { { since: nil } }

    evolve(Since) { |context, evt| context[:since] = evt.payload.on }

    command Track => Since do |_context, cmd|
      event Since, counter_id: cmd.payload.counter_id, on: cmd.payload.on
    end
  end

  # The same state without a type: mapped by hand.
  class Mapped < Sourced::Decider
    partition_by :counter_id
    consumer_group 'snapshot_spec.mapped'
    snapshot every: 1, version: 1

    context { { since: nil } }

    evolve(Since) { |context, evt| context[:since] = evt.payload.on }

    command Track => Since do |_context, cmd|
      event Since, counter_id: cmd.payload.counter_id, on: cmd.payload.on
    end

    def to_snapshot = { since: context[:since]&.iso8601 }
    def self.from_snapshot(state) = { since: state[:since] && Date.parse(state[:since]) }
  end
end

RSpec.describe Sourced::Snapshots do
  let(:db) { Sequel.sqlite }
  let(:store) { Sourced::Store.new(db) }
  let(:reactors) { [SnapshotSpec::Counter] }
  let(:router) { Sourced::Router.new(store:, reactors:) }

  before do
    store.install!
    router.setup!
  end

  def add(amount, counter_id: 'c1', reactor: SnapshotSpec::Counter)
    store.append(SnapshotSpec::Add.new(payload: { counter_id:, amount: }))
    router.handle_next_for(reactor)
  end

  def snapshots = db[:sourced_snapshots].order(:offset_id).all

  def sole_snapshot
    expect(snapshots.size).to eq(1)
    snapshots.first
  end

  def stored_state(row) = JSON.parse(row[:state], symbolize_names: true)

  def history(reactor, counter_id: 'c1', after: nil)
    store.read(reactor.history_selector.bind(counter_id:), after_position: after).messages
  end

  # The context from replaying +messages+ from the start.
  def replay(reactor, messages, counter_id: 'c1')
    reactor.snapshot_instance({ counter_id: }, nil).tap { |i| i.evolve(messages) }.context
  end

  # The context a claim would start from: the stored snapshot, then the history after it.
  def from_snapshot(reactor, counter_id: 'c1')
    snapshot = store.read_snapshot(reactor.group_id, offset_id: snapshots.last[:offset_id])
    tail = history(reactor, counter_id:, after: snapshot.position)
    reactor.snapshot_instance({ counter_id: }, snapshot).tap { |i| i.evolve(tail) }.context
  end

  describe 'the write path' do
    it 'writes none until the history has `every` messages' do
      3.times { add(1) }
      expect(snapshots).to be_empty
    end

    it 'writes the state of the history before the claim, not what the claim decided' do
      [1, 2, 3, 4].each { |n| add(n) }

      row = sole_snapshot
      third_added = history(SnapshotSpec::Counter)[2]
      expect(row[:position]).to eq(third_added.position)
      expect(row[:version]).to eq(1)
      expect(stored_state(row)).to eq(total: 6, adds: 3)
    end

    it 'matches a full replay' do
      [1, 2, 3, 4, 5, 6, 7, 8].each { |n| add(n) }

      expect(from_snapshot(SnapshotSpec::Counter)).to eq(replay(SnapshotSpec::Counter, history(SnapshotSpec::Counter)))
      expect(from_snapshot(SnapshotSpec::Counter)).to eq(total: 36, adds: 8)
    end

    it 'writes nothing when the claim fails' do
      3.times { add(1) }
      add(-1)

      expect(snapshots).to be_empty
    end

    it 'writes the snapshot when the batch fails part way' do
      3.times { add(1) }
      store.append([
        SnapshotSpec::Add.new(payload: { counter_id: 'c1', amount: 1 }),
        SnapshotSpec::Add.new(payload: { counter_id: 'c1', amount: -1 })
      ])
      router.handle_next_for(SnapshotSpec::Counter)

      expect(stored_state(sole_snapshot)).to eq(total: 3, adds: 3)
      expect(history(SnapshotSpec::Counter).size).to eq(4)
    end
  end

  describe 'the read path' do
    it 'reads only the history after the snapshot' do
      [1, 2, 3, 4].each { |n| add(n) }
      position = sole_snapshot[:position]

      allow(store).to receive(:read).and_call_original
      add(5)

      expect(store).to have_received(:read).with(anything, after_position: position)
    end

    it 'reads the whole history of groups that do not snapshot' do
      plain = Class.new(Sourced::Decider) do
        def self.name = 'SnapshotSpec::Plain'
        partition_by :counter_id
        consumer_group 'snapshot_spec.plain'
        command(SnapshotSpec::Add => SnapshotSpec::Added) { |_c, cmd| event SnapshotSpec::Added, **cmd.payload.to_h }
      end
      routed = Sourced::Router.new(store:, reactors: [plain]).setup!

      allow(store).to receive(:read).and_call_original
      allow(store).to receive(:read_snapshot)
      store.append(SnapshotSpec::Add.new(payload: { counter_id: 'c1', amount: 1 }))
      routed.handle_next_for(plain)

      expect(store).to have_received(:read).with(anything, after_position: nil)
      expect(store).not_to have_received(:read_snapshot)
    end

    it 'round-trips JSON-native Hash state with symbol keys by default' do
      [1, 2, 3, 4].each { |n| add(n) }
      snapshot = store.read_snapshot('snapshot_spec.counter', offset_id: sole_snapshot[:offset_id])
      expect(snapshot.state).to eq(total: 6, adds: 3)
    end

    it 'hands each Snapshot#state call its own copy' do
      [1, 2, 3, 4].each { |n| add(n) }
      snapshot = store.read_snapshot('snapshot_spec.counter', offset_id: sole_snapshot[:offset_id])
      snapshot.state[:total] = 100

      expect(snapshot.state).to eq(total: 6, adds: 3)
    end

    it 'ignores a snapshot of another version, and rewrites it' do
      [1, 2, 3, 4].each { |n| add(n) }
      db[:sourced_snapshots].update(version: 0)

      allow(store).to receive(:read).and_call_original
      add(5)

      expect(store).to have_received(:read).with(anything, after_position: nil)
      row = sole_snapshot
      expect(row[:version]).to eq(1)
      expect(stored_state(row)).to eq(total: 10, adds: 4)
    end

    context 'with a typed snapshot' do
      let(:strict) do
        Class.new(SnapshotSpec::Counter) do
          def self.name = 'SnapshotSpec::Strict'
          consumer_group 'snapshot_spec.strict'
          snapshot every: 3, version: 1, type: Sourced::Types::Hash[total: Integer, adds: Integer]
        end
      end
      let(:reactors) { [strict] }

      it 'ignores a snapshot that no longer decodes, and rewrites it' do
        [1, 2, 3, 4].each { |n| add(n, reactor: strict) }
        db[:sourced_snapshots].update(state: '{"total":"many"}')

        allow(store).to receive(:read).and_call_original
        add(5, reactor: strict)

        expect(store).to have_received(:read).with(anything, after_position: nil)
        expect(stored_state(sole_snapshot)).to eq(total: 10, adds: 4)
      end
    end
  end

  describe 'claimed messages that are also history' do
    let(:reactors) { [SnapshotSpec::Reacting] }

    # Commands appended together, claimed one message at a time: the claim of
    # an Added, to react to, reads the Addeds after it as history.
    def run(reactor, commands, store: self.store, router: self.router)
      store.append(commands.map { |n| SnapshotSpec::Add.new(payload: { counter_id: 'c1', amount: n }) })
      nil while router.handle_next_for(reactor, batch_size: 1)
    end

    it 'never covers a claimed message it would then skip' do
      run(SnapshotSpec::Reacting, [1, 2])
      run(SnapshotSpec::Reacting, [3])

      expect(snapshots.size).to eq(1)
      full = history(SnapshotSpec::Reacting)
      expect(from_snapshot(SnapshotSpec::Reacting)).to eq(replay(SnapshotSpec::Reacting, full))
      expect(from_snapshot(SnapshotSpec::Reacting)).to eq(total: 6, noted: 3)
    end

    it 'never covers a message a later claim evolves, so reactions decide as without snapshots' do
      plain = Class.new(SnapshotSpec::Reacting) do
        def self.name = 'SnapshotSpec::ReactingPlain'
        consumer_group 'snapshot_spec.reacting_plain'
        snapshot every: 1_000, version: 1
      end
      plain_store = Sourced::Store.new(Sequel.sqlite).tap(&:install!)
      plain_router = Sourced::Router.new(store: plain_store, reactors: [plain]).setup!
      run(plain, [1, 2, 3, 4], store: plain_store, router: plain_router)
      run(SnapshotSpec::Reacting, [1, 2, 3, 4])

      totals = ->(st, r) { st.read(r.history_selector.bind(counter_id: 'c1')).messages.grep(SnapshotSpec::Noted).map { _1.payload.total } }
      expect(snapshots).not_to be_empty
      expect(totals.(store, SnapshotSpec::Reacting)).to eq(totals.(plain_store, plain))
    end
  end

  describe 'state types' do
    let(:reactors) { [SnapshotSpec::Typed, SnapshotSpec::Mapped] }
    let(:day) { Date.new(2026, 1, 2) }

    it 'round-trips rich values through the snapshot type' do
      store.append(SnapshotSpec::Track.new(payload: { counter_id: 'c1', on: day }))
      router.handle_next_for(SnapshotSpec::Typed)
      store.append(SnapshotSpec::Track.new(payload: { counter_id: 'c1', on: day + 1 }))
      router.handle_next_for(SnapshotSpec::Typed)

      expect(stored_state(sole_snapshot)).to eq(since: '2026-01-02')
      expect(from_snapshot(SnapshotSpec::Typed)).to eq(since: day + 1)
    end

    it 'maps rich values by hand without a type' do
      store.append(SnapshotSpec::Track.new(payload: { counter_id: 'c1', on: day }))
      router.handle_next_for(SnapshotSpec::Mapped)
      store.append(SnapshotSpec::Track.new(payload: { counter_id: 'c1', on: day + 1 }))
      router.handle_next_for(SnapshotSpec::Mapped)

      expect(stored_state(sole_snapshot)).to eq(since: '2026-01-02')
      snapshot = store.read_snapshot('snapshot_spec.mapped', offset_id: sole_snapshot[:offset_id])
      expect(SnapshotSpec::Mapped.snapshot_instance({ counter_id: 'c1' }, snapshot).context).to eq(since: day)
    end

    it 'fails the claim for a state that does not fit the type' do
      unfit = Class.new(SnapshotSpec::Counter) do
        def self.name = 'SnapshotSpec::Unfit'
        consumer_group 'snapshot_spec.unfit'
        def to_snapshot = { status: :open }
      end
      routed = Sourced::Router.new(store:, reactors: [unfit]).setup!
      4.times do
        store.append(SnapshotSpec::Add.new(payload: { counter_id: 'c1', amount: 1 }))
        routed.handle_next_for(unfit)
      end

      expect(snapshots).to be_empty
      expect(history(unfit).size).to eq(3)
      expect(db[:sourced_consumer_groups].where(group_id: 'snapshot_spec.unfit').get(:status)).to eq('failed') # the default error strategy doesn't retry
    end

    it 'fails the start for a type the codec cannot encode' do
      unencodable = Class.new(SnapshotSpec::Counter) do
        def self.name = 'SnapshotSpec::Unencodable'
        consumer_group 'snapshot_spec.unencodable'
        snapshot every: 1, version: 1, type: Sourced::Types::Hash[total: Sourced::Types::Any]
      end

      expect { Sourced::Router.new(store:, reactors: [unencodable]).setup! }.to raise_error(Plumb::TypeError)
    end
  end

  describe 'the store' do
    def offset_id
      [1, 2, 3, 4].each { |n| add(n) }
      sole_snapshot[:offset_id]
    end

    it 'refuses groups registered without snapshot:' do
      store.register_consumer_group('snapshot_spec.none', Sourced::Selector[SnapshotSpec::Add].by(:counter_id))

      expect { store.read_snapshot('snapshot_spec.none', offset_id: 1) }
        .to raise_error(Sourced::Store::SnapshotsNotRegisteredError)
      expect { store.write_snapshot('snapshot_spec.none', offset_id: 1, offset_version: 1, position: 1, state: {}) }
        .to raise_error(Sourced::Store::SnapshotsNotRegisteredError)
    end

    it 'reads and writes at the version the group was last registered with' do
      id = offset_id
      selector = SnapshotSpec::Counter.claim_selector
      config = SnapshotSpec::Counter.snapshot_config
      store.register_consumer_group('snapshot_spec.counter', selector, snapshot: config.with(version: 2))

      expect(store.read_snapshot('snapshot_spec.counter', offset_id: id)).to be_nil
      version = db[:sourced_offsets].where(id:).get(:version)
      store.write_snapshot('snapshot_spec.counter', offset_id: id, offset_version: version, position: 1, state: { total: 0 })
      expect(sole_snapshot[:version]).to eq(2)
    end

    it 'writes nothing for a claim whose offset moved on' do
      id = offset_id
      version = db[:sourced_offsets].where(id:).get(:version)
      store.write_snapshot('snapshot_spec.counter', offset_id: id, offset_version: version + 1, position: 99,
                                                     state: { total: 0 })

      expect(sole_snapshot[:position]).not_to eq(99)
    end

    it 'deletes a group’s snapshots on reset, and replays rebuild them' do
      offset_id
      store.stop_consumer_group('snapshot_spec.counter')
      store.reset_consumer_group('snapshot_spec.counter')
      expect(snapshots).to be_empty

      store.start_consumer_group('snapshot_spec.counter')
      router.drain
      add(5)
      expect(snapshots.size).to eq(1)
      expect(from_snapshot(SnapshotSpec::Counter)).to eq(replay(SnapshotSpec::Counter, history(SnapshotSpec::Counter)))
    end

    it 'loses an offset’s snapshot with the offset' do
      offset_id
      store.delete_messages(db[:sourced_messages].select_map(:position))
      store.release_drained_offsets

      expect(snapshots).to be_empty
    end
  end

  describe 'definitions' do
    it 'refuses exclusive reactors' do
      reactor = Class.new(SnapshotSpec::Counter) do
        def self.name = 'SnapshotSpec::Exclusive'
        consumer_group 'snapshot_spec.exclusive'
        exclusive
      end

      expect { Sourced::Router.new(store:, reactors: [reactor]) }
        .to raise_error(Sourced::DefinitionError, /exclusive/)
    end

    it 'refuses Selector sources' do
      reactor = Class.new(Sourced::Decider) do
        def self.name = 'SnapshotSpec::Selected'
        partition_by :counter_id
        consumer_group 'snapshot_spec.selected'
        evolve Sourced::Selector[SnapshotSpec::Added]
        snapshot every: 1, version: 1
        command(SnapshotSpec::Add => SnapshotSpec::Added) { |_c, cmd| event SnapshotSpec::Added, **cmd.payload.to_h }
      end

      expect { Sourced::Router.new(store:, reactors: [reactor]) }
        .to raise_error(Sourced::DefinitionError, /inline projector/)
    end

    it 'refuses a snapshotting reactor whose handle_claim takes no snapshot' do
      reactor = Class.new do
        def self.name = 'SnapshotSpec::Duck'
        def self.claim_selector = Sourced::Selector[SnapshotSpec::Add].by(:counter_id)
        def self.snapshot_config = Sourced::Snapshots::Config.new(every: 1, version: 1, type: Sourced::Types::JSONData)
        def self.handle_claim(_claim, history:) = []
      end

      expect { Sourced::Router.new(store:, reactors: [reactor]) }
        .to raise_error(ArgumentError, /snapshot:/)
    end

    it 'refuses invalid arguments' do
      expect { Class.new(Sourced::Decider) { snapshot every: 0, version: 1 } }
        .to raise_error(Plumb::ParseError, /every/)
      expect { Class.new(Sourced::Decider) { snapshot every: 1, version: 1, type: Object.new } }
        .to raise_error(Plumb::ParseError, /type/)
    end

    it 'is inherited' do
      expect(Class.new(SnapshotSpec::Counter).snapshot_config).to eq(SnapshotSpec::Counter.snapshot_config)
      expect(Class.new(Sourced::Decider).snapshot_config).to be_nil
    end
  end
end
