# frozen_string_literal: true

require 'spec_helper'

module TestMessages
  Command = Class.new(Sourced::Message)

  Add = Command.define('test.add') do
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

  it 'defines Payload#fetch and Payload#[]' do
    msg = TestMessages::Add.new(stream_id: '123', payload: { value: 'aaa' })
    expect(msg.payload[:value]).to eq('aaa')
    expect(msg.payload.fetch(:value)).to eq('aaa')

    msg = TestMessages::Add.new(stream_id: '123')
    expect(msg.payload[:value]).to be(nil)
    expect(msg.payload.fetch(:value)).to be(nil)
    expect do
      msg.payload.fetch(:nope)
    end.to raise_error(KeyError)
  end

  it 'initializes an empty payload if the class defines one' do
    msg = TestMessages::Add.new
    expect(msg.payload).not_to be(nil)
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

  describe '.build' do
    it 'builds instance with stream_id and payload' do
      msg = TestMessages::Add.build('aaa', value: 2)
      expect(msg).to be_a(TestMessages::Add)
      expect(msg.stream_id).to eq('aaa')
      expect(msg.payload.value).to eq(2)
    end
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
      end.to raise_error(Sourced::UnknownMessageError, 'Unknown event type: test.unknown')
    end

    it 'scopes message registries by sub-class' do
      msg = TestMessages::Command.from(stream_id: '123', type: 'test.add', payload: { value: 1 })
      expect(msg).to be_a(TestMessages::Add)

      expect do
        TestMessages::Command.from(stream_id: '123', type: 'test.added', payload: { value: 1 })
      end.to raise_error(Sourced::UnknownMessageError, 'Unknown event type: test.added')
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

    it 'copies metadata' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 }, metadata: { user_id: 10 })
      added = add.follow(TestMessages::Added, add.payload)
      expect(added.metadata[:user_id]).to eq(10)
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

  describe '#at' do
    it 'creates a message with a created_at date in the future' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      delayed = add.at(add.created_at + 10)
      expect(delayed.created_at).to eq(add.created_at + 10)
    end

    it 'does not allow setting a date lower than current' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      expect do
        add.at(add.created_at - 10)
      end.to raise_error(Sourced::PastMessageDateError)
    end
  end

  describe '#to' do
    it 'creates a message with a new #stream_id' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      add2 = add.to('222')
      expect(add.stream_id).to eq('123')
      expect(add2.stream_id).to eq('222')
    end

    it 'accepts a #stream_id interface' do
      add = TestMessages::Add.new(stream_id: '123', payload: { value: 1 })
      streamable = double('Streamable', stream_id: '222')
      add2 = add.to(streamable)
      expect(add2.stream_id).to eq('222')
    end
  end
end
