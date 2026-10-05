# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'timeout'

module ConfigTestMessages
  ThingAdded = Sourced::Event.define('config_test.thing_added') do
    attribute :thing_id, String
  end
end

class ConfigTestReactor
  extend Sourced::Consumer

  partition_by :thing_id

  def self.handled_messages = [ConfigTestMessages::ThingAdded]
  def self.handle_claim(_claim) = []
end

class ConfigTestOtherReactor
  extend Sourced::Consumer

  partition_by :thing_id

  def self.group_id = 'config.test.other'
  def self.handled_messages = [ConfigTestMessages::ThingAdded]
  def self.handle_claim(_claim) = []
end

RSpec.describe Sourced::Config do
  let(:task) { CollectingTask.new }

  subject(:config) do
    described_class.build.tap do |c|
      c.config!('logger') { Sourced::NULL_LOGGER }
    end
  end

  describe '.build' do
    it 'declares every component, without building anything' do
      expect(config.index.keys).to include(
        'logger', 'db', 'notifier', 'executor', 'error_strategy', 'store', 'store.table_prefix', 'router', 'topology',
        'workers.count', 'workers.batch_size', 'workers.max_drain_rounds', 'workers.catchup_interval',
        'workers.shutdown_timeout',
        'housekeeping.interval', 'housekeeping.claim_ttl_seconds', 'dispatcher'
      )
      expect(config.boot_status).to eq(:open)
      expect { config['store'] }.to raise_error(Sourced::Component::NotBuiltError)
    end

    it 'returns a fresh tree every time' do
      expect(described_class.build).not_to be(described_class.build)
    end

    it 'has defaults for every component' do
      config.build!

      expect(config['db']).to be_a(Sequel::Database)
      expect(config['notifier']).to be_a(Sourced::InlineNotifier)
      expect(config['executor']).to be_a(Sourced::AsyncExecutor)
      expect(config['error_strategy']).to be_a(Sourced::ErrorStrategy)
      expect(config['store']).to be_a(Sourced::Store)
      expect(config['store'].db).to be(config['db'])
      expect(config['store'].notifier).to be(config['notifier'])
      expect(config['store.table_prefix']).to eq('sourced')
      expect(config['store'].installer.messages_table).to eq(:sourced_messages)
      expect(config['router']).to be_a(Sourced::Router)
      expect(config['router'].store).to be(config['store'])
      expect(config['router'].error_strategy).to be(config['error_strategy'])
      expect(config['dispatcher']).to be_a(Sourced::Dispatcher)
      expect(config['workers.count']).to eq(2)
      expect(config['workers.batch_size']).to eq(50)
      expect(config['workers.max_drain_rounds']).to eq(10)
      expect(config['workers.catchup_interval']).to eq(5)
      expect(config['workers.shutdown_timeout']).to eq(30)
      expect(config['housekeeping.interval']).to eq(30)
      expect(config['housekeeping.claim_ttl_seconds']).to eq(120)
    end

    it 'describes its dependency graph before booting' do
      described_class.register(config, ConfigTestReactor)
      components = config.graph.components.to_h { |c| [c[:key], c] }

      expect(components['store'][:deps]).to eq(%w[db notifier logger store.table_prefix])
      expect(components['router'][:deps]).to eq(%w[store reactors.ConfigTestReactor error_strategy])
      expect(components['topology'][:deps]).to eq(%w[reactors.ConfigTestReactor])
      expect(components['dispatcher'][:deps]).to include('router', 'workers.count', 'housekeeping.interval')
      expect(config.boot_status).to eq(:open)
    end
  end

  describe 'building' do
    it "doesn't touch the database" do
      db = Sequel.sqlite
      config.config!('db') { db }
      expect(db).not_to receive(:run)

      config.build!

      expect(config['store'].installed?).to be(false)
    end

    it 'builds the store with store.table_prefix' do
      config.config!('store.table_prefix') { 'billing' }
      config.config!('workers.count') { 0 }
      config.start!

      expect(config['store'].installer.messages_table).to eq(:billing_messages)
      expect(config['db'].table_exists?(:billing_messages)).to be(true)
    end

    it 'rejects a table prefix that is not an identifier' do
      config.config!('store.table_prefix') { 'billing; drop table' }

      expect { config.build! }.to raise_error(Plumb::ParseError, /store\.table_prefix/)
    end

    it 'type-checks overrides' do
      config.config!('workers.count') { -1 }

      expect { config.build! }.to raise_error(Plumb::ParseError, /workers\.count/)
    end
  end

  describe 'starting' do
    before { config.config!('workers.count') { 0 } }

    it 'prepares the store, registers consumer groups and freezes the error strategy' do
      described_class.register(config, ConfigTestReactor)
      config.start!

      expect(config['store'].installed?).to be(true)
      expect(config['store'].consumer_group_active?('ConfigTestReactor')).to be(true)
      expect(config['error_strategy']).to be_frozen
    end

    it 'spawns the dispatcher into the context it starts with' do
      config.config!('workers.count') { 2 }
      config.start!(task)

      # notifier, catch-up poller, scheduled message poller, reaper and 2 workers
      expect(task.spawned.size).to eq(6)
    ensure
      config.teardown!
    end

    it "raises for workers it can't spawn, and tears down what started" do
      config.config!('workers.count') { 2 }

      expect { config.start!(Thread.current) }.to raise_error(ArgumentError, /ThreadExecutor/)
      expect(config.boot_status).to eq(:torn_down)
      expect(config.node('store').status).to eq(:torn_down)
    end

    it 'runs no workers with workers.count 0, in any context' do
      config.start!(Thread.current)

      expect(config['dispatcher'].workers).to be_empty
    end

    it 'compiles the codec of the store' do
      klass = CodecSpecHelpers.unregistered_message('config_test.warm') do
        attribute :name, String
      end
      config.build!
      config['store'].message_codec = Sourced::Store::MessageCodec.new(
        registry: CodecSpecHelpers::Registry.new([klass])
      )

      config.start!

      expect(config['store'].message_codec.registered?('config_test.warm')).to be(true)
    end

    it 'refuses to boot when a message type cannot be serialized by the store' do
      unserializable = CodecSpecHelpers.unregistered_message('config_test.unserializable') do
        attribute :thing, Sourced::Types::Any[Object]
      end
      config.build!
      config['store'].message_codec = Sourced::Store::MessageCodec.new(
        registry: CodecSpecHelpers::Registry.new([unserializable])
      )

      expect { config.start! }.to raise_error(Plumb::TypeError, /field `payload\.thing`/)
    end

    it 'sets up the store over an overriding db' do
      db = Sequel.sqlite
      config.config!('db') { db }

      config.start!

      expect(config['store'].db).to be(db)
      expect(config['store'].installed?).to be(true)
    end

    it 'runs the lifecycle a custom store brings, which needs no setup!' do
      custom_store = Class.new do
        attr_reader :connected

        def connect = @connected = true
        def notifier = Sourced::InlineNotifier.new
        (Sourced::Config::StoreInterface.method_names - [:notifier]).each do |m|
          define_method(m) { |*, **| nil }
        end
      end.new
      config.component!('store') do
        build { custom_store }
        start { |store, _| store.connect }
      end

      config.start!

      expect(config['store']).to be(custom_store)
      expect(custom_store.connected).to be(true)
    end

    it 'freezes an overriding error strategy' do
      config.config!('error_strategy') { Sourced::ErrorStrategy.new.retry(times: 2) }

      config.start!

      expect(config['error_strategy']).to be_frozen
      expect(config['error_strategy'].max_retries).to eq(2)
    end

    it 'rejects a store that does not implement StoreInterface' do
      config.config!('store') { Object.new }

      expect { config.build! }.to raise_error(Plumb::ParseError, /store/)
    end
  end

  describe 'tearing down' do
    it 'stops dependents before their dependencies' do
      config.config!('workers.count') { 0 }
      torn_down = []
      config.notifier.subscribe('components.torn_down') { |event| torn_down << event.payload.key }

      config.start!
      config.teardown!

      expect(torn_down.index('dispatcher')).to be < torn_down.index('router')
      expect(torn_down.index('router')).to be < torn_down.index('store')
      expect(torn_down.index('store')).to be < torn_down.index('db')
    end

    it 'raises when workers are still running after the shutdown timeout, after tearing down' do
      config.config!('workers.count') { 0 }
      config.start!
      allow(config['dispatcher']).to receive(:stop).and_return(false)

      expect { config.teardown! }.to raise_error(Sourced::Dispatcher::ShutdownTimeoutError, /still running/)
      expect(config.boot_status).to eq(:torn_down)
      expect(config.node('db').status).to eq(:torn_down)
    end

    it 'disconnects the db' do
      config.config!('workers.count') { 0 }
      config.start!
      db = config['db']
      expect(db).to receive(:disconnect).and_call_original

      config.teardown!
    end
  end

  describe '.register' do
    before { config.config!('workers.count') { 0 } }

    it 'declares the reactor under reactors, keyed by group_id' do
      node = described_class.register(config, ConfigTestReactor)

      expect(node.path).to eq('reactors.ConfigTestReactor')
      expect(config.declared?('reactors.ConfigTestReactor')).to be(true)
    end

    it 'escapes characters with a meaning in component keys' do
      node = described_class.register(config, ConfigTestOtherReactor)

      expect(node.path).to eq('reactors.config_test_other')
    end

    it 'routes to every registered reactor, in registration order' do
      described_class.register(config, ConfigTestReactor)
      described_class.register(config, ConfigTestOtherReactor)
      config.start!

      expect(config['router'].reactors).to eq([ConfigTestReactor, ConfigTestOtherReactor])
    end

    it 'builds the topology of registered reactors' do
      described_class.register(config, ConfigTestReactor)
      config.build!

      expect(config['topology']).to eq(Sourced::Topology.build([ConfigTestReactor]))
    end

    it 'raises for a reactor already registered under the same key' do
      described_class.register(config, ConfigTestReactor)

      expect {
        described_class.register(config, ConfigTestReactor)
      }.to raise_error(Sourced::Component::DeclarationOverrideError, /reactors\.ConfigTestReactor/)
    end

    it 'raises once the tree is prepared' do
      config.prepare!

      expect {
        described_class.register(config, ConfigTestReactor)
      }.to raise_error(Sourced::Component::LockedComponentError)
    end

    it 'checks that implementations are reactors' do
      described_class.register(config, ConfigTestReactor)
      config.config!('reactors.ConfigTestReactor') { Object.new }

      expect { config.build! }.to raise_error(Plumb::ParseError, /reactors\.ConfigTestReactor/)
    end
  end

  describe 'mounted in a host' do
    let(:host) { Sourced::Component.new }
    let(:host_db) { Sequel.sqlite }

    before do
      db = host_db
      host.declare('db', Sequel::Database) { db }
      host.mount('sourced', config)
      host.config!('sourced.db', ['db']) { |db| db }
      host.config!('sourced.workers.count') { 0 }
    end

    it 'boots with the host, using the overrides' do
      described_class.register(config, ConfigTestReactor)
      host.start!

      expect(host['sourced.store'].db).to be(host_db)
      expect(config['store']).to be(host['sourced.store'])
      expect(config['router'].reactors).to eq([ConfigTestReactor])
    ensure
      host.teardown!
    end

    it "is booted by the host's root, not its own" do
      expect { config.start! }.to raise_error(Sourced::Component::SubcomponentError)
    end
  end
end

RSpec.describe 'Sourced.config' do
  before do
    Sourced.reset!
    Sourced.config.config!('logger') { Sourced::NULL_LOGGER }
    Sourced.config.config!('workers.count') { 0 }
  end

  after do
    Sourced.teardown! if Sourced.config.root? && Sourced.config.boot_status == :started
    Sourced.reset!
  end

  it 'is a configuration tree, memoized until reset!' do
    config = Sourced.config

    expect(config).to be_a(Sourced::Component)
    expect(config.declared?('store')).to be(true)
    expect(Sourced.config).to be(config)

    Sourced.reset!
    expect(Sourced.config).not_to be(config)
  end

  it 'is yielded by Sourced.configure' do
    returned = Sourced.configure do |c|
      c.config!('workers.batch_size') { 10 }
    end

    expect(returned).to be(Sourced.config)
    Sourced.start!
    expect(Sourced.config['workers.batch_size']).to eq(10)
  end

  it 'reads the store, router and topology once started' do
    expect { Sourced.store }.to raise_error(Sourced::Component::NotBuiltError)

    Sourced.register(ConfigTestReactor)
    Sourced.start!

    expect(Sourced.store).to be(Sourced.config['store'])
    expect(Sourced.router.reactors).to eq([ConfigTestReactor])
    expect(Sourced.topology).to eq(Sourced::Topology.build([ConfigTestReactor]))
    expect(Sourced.logger).to be(Sourced::NULL_LOGGER)
  end

  it 'tears down with Sourced.teardown!' do
    Sourced.start!
    Sourced.teardown!

    expect(Sourced.config.boot_status).to eq(:torn_down)
  end

  it 'runs workers in threads by default, and stops them on teardown' do
    Sourced.config.config!('workers.count') { 2 }
    Sourced.config.config!('workers.catchup_interval') { 0.05 }
    Sourced.config.config!('housekeeping.interval') { 0.05 }

    Sourced.start!
    workers = Sourced.config['dispatcher'].workers
    expect(workers.size).to eq(2)
    Timeout.timeout(2) { sleep 0.01 until workers.all?(&:running?) }

    Sourced.teardown!
    expect(workers.map(&:running?)).to all(be(false))
  end

  it 'manages consumer groups through the router' do
    Sourced.register(ConfigTestReactor)
    Sourced.start!

    Sourced.stop_consumer_group(ConfigTestReactor)
    expect(Sourced.store.consumer_group_active?('ConfigTestReactor')).to be(false)

    Sourced.start_consumer_group('ConfigTestReactor')
    expect(Sourced.store.consumer_group_active?('ConfigTestReactor')).to be(true)
  end

  it 'is mounted by mounting Sourced' do
    host = Sourced::Component.new
    host.mount('sourced', Sourced)
    host.start!

    expect(Sourced.store).to be(host['sourced.store'])
    expect { Sourced.start! }.to raise_error(Sourced::Component::SubcomponentError)
  ensure
    host.teardown!
  end

  describe 'Sourced.load' do
    let(:decider_class) do
      Class.new(Sourced::Decider) do
        def self.name = 'ConfigLoadDecider'

        partition_by :thing_id
        consumer_group 'config-load-decider'

        state { |_| { count: 0 } }
      end
    end

    it 'uses the configured store when store: is not provided' do
      Sourced.start!
      expect(Sourced.store).to receive(:read_partition).and_call_original

      instance, read_result = Sourced.load(decider_class, thing_id: 'abc')

      expect(instance.state[:count]).to eq(0)
      expect(read_result.messages).to be_empty
    end

    it 'uses the store: provided, without booting' do
      other_store = Sourced::Store.new(Sequel.sqlite)
      other_store.install!

      instance, read_result = Sourced.load(decider_class, store: other_store, thing_id: 'abc')

      expect(instance.state[:count]).to eq(0)
      expect(read_result.messages).to be_empty
      expect(Sourced.config.boot_status).to eq(:open)
    end
  end
end
