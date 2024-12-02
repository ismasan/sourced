# frozen_string_literal: true

require 'spec_helper'

module TestMessages
  Add = Sourced::Message.define('test.add') do
    attribute :value, Integer
  end

  Added = Sourced::Message.define('test.added') do
    attribute :value, Integer
  end
end

RSpec.describe Sourced::Message do
  it 'requires a stream_id' do
    msg = TestMessages::Add.new(payload: { value: 1 })
    expect(msg.valid?).to be false
    expect(msg.errors[:stream_id]).not_to be(nil)

    msg = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
    expect(msg.valid?).to be true
  end

  it 'validates payload' do
    msg = TestMessages::Add.new(stream_id: '123', payload: { value: 'aaa' })
    expect(msg.valid?).to be false
    expect(msg.errors[:payload][:value]).not_to be(nil)
  end

  it 'sets #type' do
    msg = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
    expect(msg.type).to eq('test.add')
  end

  it 'sets #causation_id and #correlation_id' do
    msg = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
    expect(msg.id).not_to be(nil)
    expect(msg.causation_id).to eq(msg.id)
    expect(msg.correlation_id).to eq(msg.id)
  end

  describe '.from' do
    it 'creates a message from a hash' do
      msg = Sourced::Message.from(stream_id: '123', type: 'test.add', payload: { value: 1 })
      expect(msg).to be_a(TestMessages::Add)
      expect(msg.valid?).to be(true)
    end

    it 'raises a known exception if no type found' do
      expect do
        Sourced::Message.from(stream_id: '123', type: 'test.unknown', payload: { value: 1 })
      end.to raise_error(ArgumentError, 'Unknown event type: test.unknown')
    end
  end

  describe '#follow' do
    it 'creates a new message with causation_id and correlation_id' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      added = add.follow(TestMessages::Added, value: 2)
      expect(added.causation_id).to eq(add.id)
      expect(added.correlation_id).to eq(add.id)
    end

    it 'copies payload attributes' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      added = add.follow(TestMessages::Added, add.payload)
      expect(added.payload.value).to eq(1)
    end
  end

  describe '#follow_with_seq' do
    it 'creates a new message with custom seq, causation_id and correlation_id' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      added = add.follow_with_seq(TestMessages::Added, 2, value: 2)
      expect(added.seq).to eq(2)
      expect(added.causation_id).to eq(add.id)
      expect(added.correlation_id).to eq(add.id)
    end
  end

  describe '#follow_with_stream_id' do
    it 'creates a new message with custom stream_id, causation_id and correlation_id' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      added = add.follow_with_stream_id(TestMessages::Added, 'foo', value: 2)
      expect(added.stream_id).to eq('foo')
      expect(added.causation_id).to eq(add.id)
      expect(added.correlation_id).to eq(add.id)
    end
  end
end
