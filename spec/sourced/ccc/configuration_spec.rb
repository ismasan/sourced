# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'
require 'sequel'

RSpec.describe Sourced::CCC::Configuration do
  after { Sourced::CCC.reset! }

  describe 'CCC.config' do
    it 'returns a Configuration with sensible defaults' do
      config = Sourced::CCC.config
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
      expect(Sourced::CCC.config).to be(Sourced::CCC.config)
    end
  end

  describe 'CCC.configure' do
    it 'yields the config and freezes it after setup' do
      Sourced::CCC.configure do |c|
        c.worker_count = 4
        c.batch_size = 100
      end

      expect(Sourced::CCC.config.worker_count).to eq(4)
      expect(Sourced::CCC.config.batch_size).to eq(100)
      expect(Sourced::CCC.config).to be_frozen
    end

    it 'calls setup! which creates store and router' do
      Sourced::CCC.configure {}

      expect(Sourced::CCC.config.store).to be_a(Sourced::CCC::Store)
      expect(Sourced::CCC.config.router).to be_a(Sourced::CCC::Router)
    end
  end

  describe 'CCC.register' do
    let(:reactor_class) do
      Class.new(Sourced::CCC::Projector::StateStored) do
        def self.name = 'TestConfigReactor'

        consumer_group 'test-config-reactor'
        partition_by :thing_id

        state { |_| {} }
      end
    end

    it 'triggers setup and delegates to router.register' do
      Sourced::CCC.register(reactor_class)

      expect(Sourced::CCC.router.reactors).to include(reactor_class)
    end
  end

  describe 'CCC.store' do
    it 'triggers setup and returns the store' do
      store = Sourced::CCC.store
      expect(store).to be_a(Sourced::CCC::Store)
      expect(store.installed?).to be true
    end
  end

  describe 'CCC.router' do
    it 'triggers setup and returns the router' do
      router = Sourced::CCC.router
      expect(router).to be_a(Sourced::CCC::Router)
      expect(router.store).to be(Sourced::CCC.store)
    end
  end

  describe 'CCC.reset!' do
    it 'clears the singleton config' do
      original = Sourced::CCC.config
      Sourced::CCC.reset!
      expect(Sourced::CCC.config).not_to be(original)
    end
  end

  describe '#store=' do
    it 'accepts a CCC::Store instance directly' do
      db = Sequel.sqlite
      store = Sourced::CCC::Store.new(db)

      config = described_class.new
      config.store = store
      expect(config.store).to be(store)
    end

    it 'wraps a Sequel::SQLite::Database in a Store' do
      db = Sequel.sqlite

      config = described_class.new
      config.store = db
      expect(config.store).to be_a(Sourced::CCC::Store)
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
    it 'falls through to Sourced.config.error_strategy by default' do
      config = described_class.new
      expect(config.error_strategy).to eq(Sourced.config.error_strategy)
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
      expect(config.store).to be_a(Sourced::CCC::Store)
      expect(config.store.installed?).to be true
    end

    it 'uses configured store when set' do
      db = Sequel.sqlite
      store = Sourced::CCC::Store.new(db)
      store.install!

      config = described_class.new
      config.store = store
      config.setup!
      expect(config.store).to be(store)
    end
  end

  describe 'CCC.load with global store' do
    let(:db) { Sequel.sqlite }
    let(:store) { Sourced::CCC::Store.new(db) }

    let(:decider_class) do
      Class.new(Sourced::CCC::Decider) do
        def self.name = 'ConfigLoadDecider'

        partition_by :thing_id
        consumer_group 'config-load-decider'

        state { |_| { count: 0 } }
      end
    end

    before do
      store.install!
      Sourced::CCC.configure do |c|
        c.store = store
      end
    end

    it 'uses global store when store: not provided' do
      instance, read_result = Sourced::CCC.load(decider_class, thing_id: 'abc')
      expect(instance.state[:count]).to eq(0)
      expect(read_result.messages).to be_empty
    end

    it 'uses override store when store: provided' do
      other_db = Sequel.sqlite
      other_store = Sourced::CCC::Store.new(other_db)
      other_store.install!

      instance, read_result = Sourced::CCC.load(decider_class, store: other_store, thing_id: 'abc')
      expect(instance.state[:count]).to eq(0)
      expect(read_result.messages).to be_empty
    end
  end
end
