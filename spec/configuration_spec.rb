# frozen_string_literal: true

require 'sequel'

RSpec.describe Sors::Configuration do
  subject(:config) { described_class.new }

  it 'has a test backend by default' do
    expect(config.backend).to be_a(Sors::Backends::TestBackend)
  end

  describe '#backend=' do
    it 'can configure backend with a Sequel database' do
      config.backend = Sequel.sqlite
      expect(config.backend).to be_a(Sors::Backends::SequelBackend)
    end

    it 'accepts anything with the Backend interface' do
      backend = Struct.new(
        :installed?,
        :reserve_next_for,
        :append_to_stream,
        :read_event_batch,
        :read_event_stream,
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
