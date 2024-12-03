# frozen_string_literal: true

require 'spec_helper'

module ContextTest
  Add = Sourced::Command.define('ctest.add') do
    attribute :value, Integer
  end

  Added = Sourced::Event.define('ctest.added')
end

RSpec.describe Sourced::CommandContext do
  describe '#build' do
    it 'builds command with stream_id and metadata' do
      ctx = described_class.new(stream_id: '123', metadata: { user_id: 10 })
      cmd = ctx.build(type: 'ctest.add', payload: { value: 1 })
      expect(cmd).to be_a(ContextTest::Add)
      expect(cmd.stream_id).to eq('123')
      expect(cmd.payload.value).to eq(1)
      expect(cmd.metadata[:user_id]).to eq(10)
    end

    it 'allows overriding stream_id' do
      ctx = described_class.new(stream_id: '123', metadata: { user_id: 10 })
      cmd = ctx.build(stream_id: 'aaa', type: 'ctest.add', payload: { value: 1 })
      expect(cmd.stream_id).to eq('aaa')
    end

    it 'symbolizes attributes' do
      ctx = described_class.new(stream_id: '123', metadata: { user_id: 10 })
      cmd = ctx.build('type' => 'ctest.add', 'payload' => { 'value' => 1 })
      expect(cmd).to be_a(ContextTest::Add)
      expect(cmd.stream_id).to eq('123')
      expect(cmd.payload.value).to eq(1)
      expect(cmd.metadata[:user_id]).to eq(10)
    end

    it 'raises an exception if command type does not exist' do
      ctx = described_class.new(stream_id: '123', metadata: { user_id: 10 })
      expect do
        ctx.build('type' => 'nope', 'payload' => { 'value' => 1 })
      end.to raise_error(Sourced::UnknownMessageError)
    end

    it 'raises an exception if command type does not exist in scope class' do
      ctx = described_class.new(stream_id: '123', metadata: { user_id: 10 })
      expect do
        ctx.build('type' => 'ctest.added', 'payload' => { 'value' => 1 })
      end.to raise_error(Sourced::UnknownMessageError)
    end
  end
end
