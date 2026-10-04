# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

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
  # A task that collects what the dispatcher spawns, without running it
  let(:task) do
    Class.new do
      attr_reader :spawned

      def initialize = @spawned = []

      def spawn(&block)
        @spawned << block
        self
      end
    end.new
  end

  subject(:config) do
    described_class.build.tap do |c|
      c.config!('logger') { Sourced::NULL_LOGGER }
    end
  end

  describe '.build' do
    it 'declares every component, without building anything' do
      expect(config.index.keys).to include(
        'logger', 'db', 'notifier', 'executor', 'error_strategy', 'store', 'router', 'topology',
        'workers.count', 'workers.batch_size', 'workers.max_drain_rounds', 'workers.catchup_interval',
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
      expect(config['router']).to be_a(Sourced::Router)
      expect(config['router'].store).to be(config['store'])
      expect(config['router'].error_strategy).to be(config['error_strategy'])
      expect(config['dispatcher']).to be_a(Sourced::Dispatcher)
      expect(config['workers.count']).to eq(2)
      expect(config['workers.batch_size']).to eq(50)
      expect(config['workers.max_drain_rounds']).to eq(10)
      expect(config['workers.catchup_interval']).to eq(5)
      expect(config['housekeeping.interval']).to eq(30)
      expect(config['housekeeping.claim_ttl_seconds']).to eq(120)
    end

    it 'describes its dependency graph before booting' do
      described_class.register(config, ConfigTestReactor)
      components = config.graph.components.to_h { |c| [c[:key], c] }

      expect(components['store'][:deps]).to eq(%w[db notifier logger])
      expect(components['router'][:deps]).to eq(%w[store reactors.ConfigTestReactor error_strategy])
      expect(components['topology'][:deps]).to eq(%w[reactors.ConfigTestReactor])
      expect(components['dispatcher'][:deps]).to include('router', 'executor', 'workers.count')
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

      expect { config.start!(Thread.current) }.to raise_error(ArgumentError, /Supervisor/)
      expect(config.boot_status).to eq(:torn_down)
      expect(config.node('store').status).to eq(:torn_down)
    end

    it 'runs no workers with workers.count 0, in any context' do
      config.start!(Thread.current)

      expect(config['dispatcher'].workers).to be_empty
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

    it 'raises for a group_id that is already registered' do
      described_class.register(config, ConfigTestReactor)

      expect {
        described_class.register(config, ConfigTestReactor)
      }.to raise_error(ArgumentError, /group_id "ConfigTestReactor" is already registered/)
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
