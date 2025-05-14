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
    it 'can configure backend with a Sequel database' do
      config.backend = Sequel.sqlite
      expect(config.backend).to be_a(Sourced::Backends::SequelBackend)
    end

    it 'accepts anything with the Backend interface' do
      backend = Struct.new(
        :installed?,
        :reserve_next_for_reactor,
        :append_to_stream,
        :read_correlation_batch,
        :read_event_stream,
        :schedule_commands,
        :next_command,
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
end
