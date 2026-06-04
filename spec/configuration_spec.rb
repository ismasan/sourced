# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sequel'

RSpec.describe Sourced::Configuration do
  after { Sourced.reset! }

  describe 'Sourced.config' do
    it 'returns a Configuration with sensible defaults' do
      config = Sourced.config
      expect(config).to be_a(described_class)
      expect(config.worker_count).to eq(2)
      expect(config.batch_size).to eq(50)
      expect(config.catchup_interval).to eq(5)
      expect(config.max_drain_rounds).to eq(10)
      expect(config.claim_ttl_seconds).to eq(120)
      expect(config.housekeeping_interval).to eq(30)
      expect(config.logger).to eq(Sourced.config.logger)
    end

    it 'returns the same instance on repeated calls' do
      expect(Sourced.config).to be(Sourced.config)
    end
  end

  describe 'Sourced.configure' do
    it 'yields the config and leaves it mutable (does not freeze)' do
      Sourced.configure do |c|
        c.worker_count = 4
        c.batch_size = 100
      end

      expect(Sourced.config.worker_count).to eq(4)
      expect(Sourced.config.batch_size).to eq(100)
      expect(Sourced.config).not_to be_frozen
    end

    it 'calls setup! which creates store and router' do
      Sourced.configure {}

      expect(Sourced.config.store).to be_a(Sourced::Store)
      expect(Sourced.config.router).to be_a(Sourced::Router)
    end
  end

  describe 'Sourced.register' do
    let(:reactor_class) do
      Class.new(Sourced::Projector::StateStored) do
        def self.name = 'TestConfigReactor'

        consumer_group 'test-config-reactor'
        partition_by :thing_id

        state { |_| {} }
      end
    end

    it 'triggers setup and delegates to router.register' do
      Sourced.register(reactor_class)

      expect(Sourced.router.reactors).to include(reactor_class)
    end
  end

  describe 'Sourced.store' do
    it 'triggers setup and returns the store' do
      store = Sourced.store
      expect(store).to be_a(Sourced::Store)
      expect(store.installed?).to be true
    end
  end

  describe 'Sourced.router' do
    it 'triggers setup and returns the router' do
      router = Sourced.router
      expect(router).to be_a(Sourced::Router)
      expect(router.store).to be(Sourced.store)
    end
  end

  describe 'Sourced.setup!' do
    let(:reactor_class) do
      Class.new(Sourced::Projector::StateStored) do
        def self.name = 'SetupTestReactor'

        consumer_group 'setup-test-reactor'
        partition_by :thing_id

        state { |_| {} }
      end
    end

    it 'replays the configure block on the reused Configuration' do
      call_count = 0
      Sourced.configure do |c|
        call_count += 1
        c.worker_count = 8
      end

      expect(call_count).to eq(1)
      original_config = Sourced.config

      Sourced.setup!

      expect(call_count).to eq(2)
      expect(Sourced.config).to be(original_config)
      expect(Sourced.config.worker_count).to eq(8)
      expect(Sourced.config).to be_frozen
    end

    it 'accumulates multiple configure blocks and replays all of them' do
      first_calls = 0
      second_calls = 0

      Sourced.configure do |c|
        first_calls += 1
        c.worker_count = 4
      end
      Sourced.configure do |c|
        second_calls += 1
        c.batch_size = 100
      end

      # Each block applied immediately to the shared config instance.
      expect(first_calls).to eq(1)
      expect(second_calls).to eq(1)
      expect(Sourced.config.worker_count).to eq(4)
      expect(Sourced.config.batch_size).to eq(100)

      Sourced.setup!

      # Both blocks re-ran against the same config.
      expect(first_calls).to eq(2)
      expect(second_calls).to eq(2)
      expect(Sourced.config.worker_count).to eq(4)
      expect(Sourced.config.batch_size).to eq(100)
    end

    it 'creates a new store connection on each call' do
      Sourced.configure {}
      store1 = Sourced.config.store

      Sourced.setup!
      store2 = Sourced.config.store

      expect(store2).not_to be(store1)
    end

    it 'works without a configure block' do
      Sourced.setup!

      expect(Sourced.config.store).to be_a(Sourced::Store)
      expect(Sourced.config.router).to be_a(Sourced::Router)
      expect(Sourced.config).to be_frozen
    end

    it 're-registers reactors on the rebuilt router (survive fork re-setup)' do
      Sourced.configure {}
      Sourced.register(reactor_class)
      expect(Sourced.config.router.reactors).to include(reactor_class)

      Sourced.setup!

      # Router was rebuilt around a fresh store, but the reactor is still
      # registered (and its consumer group re-registered against the new store).
      expect(Sourced.config.router.reactors).to eq([reactor_class])
      expect(Sourced.config.store.consumer_group_active?(reactor_class)).to be(true)
    end

    it 'preserves error_strategy callbacks registered outside the configure block' do
      Sourced.configure do |c|
        c.error_strategy.retry(times: 2)
      end

      fired = []
      Sourced.config.error_strategy.on_retry { |retry_count:, **| fired << retry_count }

      Sourced.setup!

      # Block retry config re-applied, and the externally-registered callback
      # survives the re-setup and still fires.
      strategy = Sourced.config.error_strategy
      expect(strategy.max_retries).to eq(2)
      expect(strategy).to be_frozen

      group = instance_double('Group')
      allow(group).to receive(:error_context).and_return(retry_count: 1)
      allow(group).to receive(:retry)
      strategy.call(RuntimeError.new('boom'), double('Message'), group)

      expect(fired).to eq([1])
    end
  end

  describe 'Sourced.reset!' do
    it 'clears the singleton config' do
      original = Sourced.config
      Sourced.reset!
      expect(Sourced.config).not_to be(original)
    end

    it 'clears the stored configure block' do
      Sourced.configure do |c|
        c.worker_count = 8
      end

      Sourced.reset!
      Sourced.setup!

      expect(Sourced.config.worker_count).to eq(2)
    end
  end

  describe '#store=' do
    it 'accepts a Sourced::Store instance directly' do
      db = Sequel.sqlite
      store = Sourced::Store.new(db)

      config = described_class.new
      config.store = store
      expect(config.store).to be(store)
    end

    it 'wraps a Sequel::SQLite::Database in a Store' do
      db = Sequel.sqlite

      config = described_class.new
      config.store = db
      expect(config.store).to be_a(Sourced::Store)
      expect(config.store.db).to be(db)
    end

    it 'accepts any object implementing StoreInterface' do
      fake_store = double('CustomStore',
        installed?: true, install!: nil, append: nil, read: nil,
        read_partition: nil, claim_next: nil, ack: nil, release: nil,
        register_consumer_group: nil, worker_heartbeat: nil,
        release_stale_claims: nil, notifier: nil
      )

      config = described_class.new
      config.store = fake_store
      expect(config.store).to be(fake_store)
    end

    it 'raises for objects not implementing StoreInterface' do
      config = described_class.new
      expect { config.store = Object.new }.to raise_error(Plumb::ParseError)
    end
  end

  describe '#error_strategy' do
    it 'returns a default ErrorStrategy' do
      config = described_class.new
      expect(config.error_strategy).to be_a(Sourced::ErrorStrategy)
    end

    it 'can be overridden with a custom callable' do
      custom = ->(_e, _m, _g) {}
      config = described_class.new
      config.error_strategy = custom
      expect(config.error_strategy).to be(custom)
    end

    it 'raises if assigned a non-callable' do
      config = described_class.new
      expect { config.error_strategy = 'not callable' }.to raise_error(ArgumentError)
    end

    it 'is configured by mutating the strategy in place' do
      config = described_class.new
      config.error_strategy.retry(times: 5, after: 10)

      expect(config.error_strategy).to be_a(Sourced::ErrorStrategy)
      expect(config.error_strategy.max_retries).to eq(5)
      expect(config.error_strategy.retry_after).to eq(10)
    end

    it 'stays mutable so callbacks can be registered after retry config' do
      config = described_class.new
      config.error_strategy.retry(times: 3)
      expect { config.error_strategy.on_retry { } }.not_to raise_error
    end

    it 'is frozen when the configuration is frozen' do
      config = described_class.new
      config.freeze

      expect(config.error_strategy).to be_frozen
      expect { config.error_strategy.on_fail { } }.to raise_error(FrozenError)
    end
  end

  describe '#setup!' do
    it 'is idempotent' do
      config = described_class.new
      config.setup!
      store1 = config.store
      router1 = config.router
      config.setup!
      expect(config.store).to be(store1)
      expect(config.router).to be(router1)
    end

    it 'defaults to in-memory SQLite store when none configured' do
      config = described_class.new
      config.setup!
      expect(config.store).to be_a(Sourced::Store)
      expect(config.store.installed?).to be true
    end

    it 'uses configured store when set' do
      db = Sequel.sqlite
      store = Sourced::Store.new(db)
      store.install!

      config = described_class.new
      config.store = store
      config.setup!
      expect(config.store).to be(store)
    end
  end

  describe 'Sourced.load with global store' do
    let(:db) { Sequel.sqlite }
    let(:store) { Sourced::Store.new(db) }

    let(:decider_class) do
      Class.new(Sourced::Decider) do
        def self.name = 'ConfigLoadDecider'

        partition_by :thing_id
        consumer_group 'config-load-decider'

        state { |_| { count: 0 } }
      end
    end

    before do
      store.install!
      Sourced.configure do |c|
        c.store = store
      end
    end

    it 'uses global store when store: not provided' do
      instance, read_result = Sourced.load(decider_class, thing_id: 'abc')
      expect(instance.state[:count]).to eq(0)
      expect(read_result.messages).to be_empty
    end

    it 'uses override store when store: provided' do
      other_db = Sequel.sqlite
      other_store = Sourced::Store.new(other_db)
      other_store.install!

      instance, read_result = Sourced.load(decider_class, store: other_store, thing_id: 'abc')
      expect(instance.state[:count]).to eq(0)
      expect(read_result.messages).to be_empty
    end
  end
end
