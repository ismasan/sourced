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

  describe '.dispatch(message)' do
    before(:all) do
      @message_class = Sourced::Message.define('dispatch.test') do
        attribute :name, String
      end
    end

    it 'appends message' do
      msg = @message_class.parse(stream_id: 'aaa', payload: { name: 'Joe' })
      expect(Sourced.dispatch(msg)).to eq(msg)
      expect(Sourced.config.backend.read_stream('aaa').map(&:id)).to eq([msg.id])
    end

    it 'raises if message is invalid' do
      msg = @message_class.new(stream_id: 'aaa', payload: { name: 22 })
      expect {
        Sourced.dispatch(msg)
      }.to raise_error(Sourced::InvalidMessageError)
    end

    it 'raises if backend fails to append' do
      msg = @message_class.parse(stream_id: 'aaa', payload: { name: 'Joe' })
      allow(Sourced.config.backend).to receive(:append_next_to_stream).and_return false
      expect {
        Sourced.dispatch(msg)
      }.to raise_error(Sourced::BackendError)
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
