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

  specify '.registered?' do
    reactor1 = Class.new do
      extend Sourced::Consumer

      consumer do |info|
        info.group_id = 'reactor1'
      end

      def self.handled_messages = [Sourced::Event]
      def self.handle(...) = []
    end

    reactor2 = Class.new do
      extend Sourced::Consumer

      consumer do |info|
        info.group_id = 'reactor2'
      end

      def self.handled_messages = [Sourced::Event]
      def self.handle(...) = []
    end

    Sourced.register(reactor1)

    expect(Sourced.registered?(reactor1)).to be true
    expect(Sourced.registered?(reactor2)).to be false
  end
end
