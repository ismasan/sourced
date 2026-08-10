# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'bigdecimal'
require 'date'
require 'json'

module MessageCodecTests
  Rich = Sourced::Message.define('message_codec_test.rich') do
    attribute :id, String
    attribute :on, Date
    attribute :at, Time
    attribute :level, Sourced::Types::Symbol
    attribute :amount, Sourced::Types::Decimal
    attribute? :note, String
  end

  Nested = Sourced::Message.define('message_codec_test.nested') do
    attribute :items, Sourced::Types::Array do
      attribute :sku, String
      attribute :ships_on, Sourced::Types::Date.nullable
    end
  end

  Native = Sourced::Message.define('message_codec_test.native') do
    attribute :name, String
    attribute :count, Integer
  end

  Payloadless = Sourced::Message.define('message_codec_test.payloadless')

  # A value type with an encoder, in a format scoped to this spec so that the
  # global one stays free of it.
  Money = Data.define(:cents, :currency)

  class MoneyEncoder < Plumb::Encoder[
    Sourced::Types::Hash[cents: Integer, currency: String] => Sourced::Types::Any[Money]
  ]
    def encode(money) = { cents: money.cents, currency: money.currency }
    def decode(hash) = Money.new(cents: hash[:cents], currency: hash[:currency])
  end

  class AppCodec < Plumb::Codec::JSON
    encoder MoneyEncoder
  end

  # Unregistered: the default codec can't serialize it, and every
  # Sourced.setup! in the suite walks the global registry.
  Priced = CodecSpecHelpers.unregistered_message('message_codec_test.priced') do
    attribute :price, Sourced::Types::Any[Money]
  end

  # Unserializable by any codec — Types::Any names nothing to encode.
  Opaque = CodecSpecHelpers.unregistered_message('message_codec_test.opaque') do
    attribute :thing, Sourced::Types::Any[Object]
  end
end

RSpec.describe Sourced::Store::MessageCodec do
  # Scoped to the types these specs use, and compiled the way Store#setup! does.
  subject(:codec) { codec_for(TYPES) }

  TYPES = [
    MessageCodecTests::Rich,
    MessageCodecTests::Nested,
    MessageCodecTests::Native,
    MessageCodecTests::Payloadless
  ].freeze

  def codec_for(classes, format: Plumb::Codec::JSON)
    described_class.new(format:, registry: CodecSpecHelpers::Registry.new(classes)).compile!
  end

  # Round-trip a message the way the store does: envelope by hand, payload
  # through the codec, all of it through JSON, then back.
  def round_trip(message, with: codec)
    data = {
      id: message.id,
      type: message.type,
      causation_id: message.causation_id,
      correlation_id: message.correlation_id,
      created_at: message.created_at.iso8601(6),
      metadata: message.metadata,
      payload: with.encode(message)
    }
    with.decode(JSON.parse(JSON.dump(data), symbolize_names: true))
  end

  describe 'native Ruby types' do
    let(:message) do
      MessageCodecTests::Rich.new(payload: {
                                    id: 'r1',
                                    on: Date.new(1978, 10, 8),
                                    at: Time.at(1_700_000_000, 123_456),
                                    level: :warning,
                                    amount: BigDecimal('10.55')
                                  })
    end

    it 'encodes payload values into JSON-native ones' do
      expect(codec.encode(message)).to eq(
        id: 'r1',
        on: '1978-10-08',
        at: Time.at(1_700_000_000, 123_456).iso8601(6),
        level: 'warning',
        amount: '10.55'
      )
    end

    it 'decodes them back into Ruby values' do
      payload = round_trip(message).payload

      expect(payload.on).to eq(Date.new(1978, 10, 8))
      expect(payload.level).to eq(:warning)
      expect(payload.amount).to eq(BigDecimal('10.55'))
      expect(payload.amount).to be_a(BigDecimal)
    end

    it 'preserves Time to microsecond precision' do
      at = round_trip(message).payload.at

      expect(at).to be_a(Time)
      expect(at.to_i).to eq(1_700_000_000)
      expect(at.usec).to eq(123_456)
    end

    it 'omits optional attributes that were never set, rather than writing nulls' do
      expect(codec.encode(message)).not_to have_key(:note)
      expect(round_trip(message).payload.note).to be_nil
    end
  end

  describe 'the envelope' do
    let(:message) { MessageCodecTests::Native.new(payload: { name: 'x', count: 1 }) }

    it 'is not the codec\'s business — only the payload is encoded' do
      expect(codec.encode(message)).to eq(name: 'x', count: 1)
    end

    it 'is carried through #decode, which builds the message from it' do
      loaded = round_trip(message)

      expect(loaded).to be_a(MessageCodecTests::Native)
      expect(loaded.id).to eq(message.id)
      expect(loaded.causation_id).to eq(message.causation_id)
      expect(loaded.correlation_id).to eq(message.correlation_id)
      expect(loaded.created_at.usec).to eq(message.created_at.usec)
    end

    it 'carries metadata through' do
      with_meta = message.with_metadata(user_id: 42, source: 'test')

      expect(round_trip(with_meta).metadata).to eq(user_id: 42, source: 'test')
    end
  end

  describe 'nested structures' do
    let(:message) do
      MessageCodecTests::Nested.new(payload: {
                                      items: [
                                        { sku: 'a', ships_on: Date.new(2026, 1, 1) },
                                        { sku: 'b', ships_on: nil }
                                      ]
                                    })
    end

    it 'encodes nested structs inside arrays' do
      expect(codec.encode(message)).to eq(
        items: [{ sku: 'a', ships_on: '2026-01-01' }, { sku: 'b', ships_on: nil }]
      )
    end

    it 'decodes them back, nullables included' do
      items = round_trip(message).payload.items

      expect(items.map(&:sku)).to eq(%w[a b])
      expect(items.first.ships_on).to eq(Date.new(2026, 1, 1))
      expect(items.last.ships_on).to be_nil
    end
  end

  describe 'messages defined without a payload' do
    let(:message) { MessageCodecTests::Payloadless.new }

    it 'encodes to nil' do
      expect(codec.encode(message)).to be_nil
    end

    it 'round-trips' do
      loaded = round_trip(message)

      expect(loaded).to be_a(MessageCodecTests::Payloadless)
      expect(loaded.payload).to be_nil
    end
  end

  describe '#compile!' do
    it 'registers a pair per message type' do
      registry = CodecSpecHelpers::Registry.new([MessageCodecTests::Rich, MessageCodecTests::Native])
      codec = described_class.new(registry:)

      expect(codec.registered?('message_codec_test.rich')).to be(false)
      expect(codec.compile!).to be(codec)
      expect(codec.registered?('message_codec_test.rich')).to be(true)
      expect(codec.registered?('message_codec_test.native')).to be(true)
    end

    it 'raises for a message type it cannot represent, naming the attribute' do
      registry = CodecSpecHelpers::Registry.new([MessageCodecTests::Native, MessageCodecTests::Opaque])
      codec = described_class.new(registry:)

      expect { codec.compile! }.to raise_error(Plumb::TypeError, /field `thing`/)
    end

    it 'picks up encoders added to the format before it runs' do
      format = Class.new(Plumb::Codec::JSON)
      codec = described_class.new(
        format:,
        registry: CodecSpecHelpers::Registry.new([MessageCodecTests::Priced])
      )
      format.encoder MessageCodecTests::MoneyEncoder

      expect { codec.compile! }.not_to raise_error
      expect(codec.registered?(MessageCodecTests::Priced.type)).to be(true)
    end

    it 'short-circuits once compiled, so collaborators can each call it' do
      codec = codec_for([MessageCodecTests::Native])
      expect { codec.compile! }.not_to change(codec, :compiled?)
    end
  end

  describe '#recompile!' do
    it 'picks up message types defined since the last compile' do
      registry = CodecSpecHelpers::Registry.new(classes = [MessageCodecTests::Native])
      codec = described_class.new(registry:).compile!
      classes << MessageCodecTests::Rich

      expect { codec.recompile! }.to change { codec.registered?('message_codec_test.rich') }.to(true)
    end
  end

  describe 'a message type that was not compiled' do
    let(:late) do
      CodecSpecHelpers.unregistered_message('message_codec_test.late') { attribute :name, String }
    end

    # Known as a message type, absent from the pairs, which were compiled first.
    let(:codec) do
      classes = [MessageCodecTests::Native]
      codec_for(classes).tap { classes << late }
    end

    it 'raises on encode rather than compiling mid-request' do
      expect {
        codec.encode(late.new(payload: { name: 'x' }))
      }.to raise_error(Sourced::Message::JSONCodec::UnregisteredTypeError, /message_codec_test\.late/)
    end

    it 'raises on decode' do
      expect {
        codec.decode(id: 'abc', type: late.type, payload: { name: 'x' })
      }.to raise_error(Sourced::Message::JSONCodec::UnregisteredTypeError, /message_codec_test\.late/)
    end
  end

  describe '#decode' do
    it 'raises for an unregistered message type rather than dropping its payload' do
      expect {
        codec.decode(id: 'abc', type: 'message_codec_test.not_registered', payload: { a: 1 })
      }.to raise_error(Sourced::Message::UnknownMessageError, /message_codec_test\.not_registered \(abc\)/)
    end

    it 'raises DecodeError when a stored payload no longer fits its schema' do
      expect {
        codec.decode(id: 'abc', type: 'message_codec_test.rich', payload: { id: 'r1', on: 'not-a-date' })
      }.to raise_error(described_class::DecodeError, /message_codec_test\.rich \(abc\)/)
    end
  end

  describe '#encode_payload' do
    it 'raises EncodeError for a message whose payload does not fit its schema' do
      invalid = MessageCodecTests::Native.new(payload: { name: 'x', count: 'not-an-integer' })

      expect(invalid).not_to be_valid
      expect { codec.encode(invalid) }.to raise_error(described_class::EncodeError, /count/)
    end
  end

  describe 'app-registered encoders' do
    subject(:codec) { codec_for([MessageCodecTests::Priced], format: MessageCodecTests::AppCodec) }

    let(:money) { MessageCodecTests::Money.new(cents: 1050, currency: 'GBP') }
    let(:message) { MessageCodecTests::Priced.new(payload: { price: money }) }

    it 'encodes app value types through the registered encoder' do
      expect(codec.encode(message)).to eq(price: { cents: 1050, currency: 'GBP' })
    end

    it 'decodes them back' do
      expect(round_trip(message).payload.price).to eq(money)
    end

    it 'is unknown to the default codec' do
      expect { codec_for([MessageCodecTests::Priced]) }.to raise_error(Plumb::TypeError, /field `price`/)
    end
  end

  describe '.default' do
    it 'is shared, so stores of this layout compile pairs once per process' do
      expect(described_class.default).to be(described_class.default)
    end
  end
end
