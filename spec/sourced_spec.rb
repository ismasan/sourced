# frozen_string_literal: true

RSpec.describe Sourced do
  it 'has a version number' do
    expect(Sourced::VERSION).not_to be nil
  end

  describe '.new_stream_id' do
    specify 'no prefix' do
      si1 = Sourced.new_stream_id
      si2 = Sourced.new_stream_id
      expect(si1).not_to eq(si2)
    end

    specify 'with prefix' do
      si1 = Sourced.new_stream_id('cart')
      si2 = Sourced.new_stream_id('cart')
      expect(si1).not_to eq(si2)
      expect(si1).to start_with('cart')
    end
  end
end
