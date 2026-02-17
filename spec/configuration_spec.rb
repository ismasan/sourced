# frozen_string_literal: true

require 'sequel'

RSpec.describe Sourced::Configuration do
  subject(:config) { described_class.new }

  it 'has a test backend by default' do
    expect(config.backend).to be_a(Sourced::Backends::TestBackend)
  end

  it 'has a default #error_strategy' do
    expect(config.error_strategy).to be_a(Sourced::ErrorStrategy)
  end

  specify '#error_strategy=' do
    st = Sourced::ErrorStrategy.new
    config.error_strategy = st
    expect(config.error_strategy).to eq(st)
  end

  describe '#error_strategy(&block)' do
    it 'configures the error strategy with a block' do
      config.error_strategy do |s|
        s.retry(times: 30, after: 50)
      end

      expect(config.error_strategy).to be_a(Sourced::ErrorStrategy)
      expect(config.error_strategy.max_retries).to eq(30)
      expect(config.error_strategy.retry_after).to eq(50)
    end
  end

  describe '#backend=' do
    it 'can configure backend with a Sequel SQLite database' do
      config.backend = Sequel.sqlite
      expect(config.backend).to be_a(Sourced::Backends::SQLiteBackend)
    end

    it 'accepts anything with the Backend interface' do
      backend = Struct.new(
        :installed?,
        :reserve_next_for_reactor,
        :append_to_stream,
        :read_correlation_batch,
        :read_stream,
        :updating_consumer_group,
        :register_consumer_group,
        :start_consumer_group,
        :stop_consumer_group,
        :reset_consumer_group,
        :stats,
        :transaction
      )

      config.backend = backend.new(nil, nil, nil, nil, nil, nil)

      expect(config.backend).to be_a(backend)
    end

    it 'fails loudly if the backend does not implement the Backend interface' do
      expect { config.backend = Object.new }.to raise_error(Plumb::ParseError)
    end
  end

  describe '#pubsub' do
    it 'defaults to PubSub::Test' do
      expect(config.pubsub).to be_a(Sourced::PubSub::Test)
    end

    it 'auto-sets PubSub::PG when backend is a Postgres database' do
      config.backend = Sequel.postgres('sourced_test')
      expect(config.pubsub).to be_a(Sourced::PubSub::PG)
    end

    it 'keeps current pubsub when backend is SQLite' do
      config.backend = Sequel.sqlite
      expect(config.pubsub).to be_a(Sourced::PubSub::Test)
    end

    it 'can be overridden with pubsub=' do
      custom = Struct.new(:subscribe, :publish).new
      config.pubsub = custom
      expect(config.pubsub).to eq(custom)
    end

    it 'fails loudly if the pubsub does not implement the PubSub interface' do
      expect { config.pubsub = Object.new }.to raise_error(Plumb::ParseError)
    end
  end

  specify '#executor' do
    expect(config.executor).to be_a(Sourced::AsyncExecutor)
  end

  describe 'subscribers' do
    it 'triggers subscribers on #setup!' do
      executor_class = nil
      config.subscribe do |c|
        executor_class = c.executor.class
      end
      config.executor = :thread
      config.setup!
      expect(executor_class).to eq(Sourced::ThreadExecutor)
    end
  end

  describe '#executor=()' do
    specify ':async' do
      config.executor = :async
      expect(config.executor).to be_a(Sourced::AsyncExecutor)
    end

    specify ':thread' do
      config.executor = :thread
      expect(config.executor).to be_a(Sourced::ThreadExecutor)
    end

    specify 'any #start interface' do
      custom = Class.new do
        def start; end
      end

      config.executor = custom.new
      expect(config.executor).to be_a(custom)
    end

    specify 'an invalid executor' do
      expect {
        config.executor = Object.new
      }.to raise_error(ArgumentError)
    end
  end
end
