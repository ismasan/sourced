# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

module CccContextTest
  Add = Sourced::CCC::Command.define('ccc_ctest.add') do
    attribute :value, Integer
  end

  Added = Sourced::CCC::Event.define('ccc_ctest.added')
end

RSpec.describe Sourced::CCC::CommandContext do
  describe '#build' do
    it 'builds command from type string with metadata' do
      ctx = described_class.new(metadata: { user_id: 10 })
      cmd = ctx.build(type: 'ccc_ctest.add', payload: { value: 1 })
      expect(cmd).to be_a(CccContextTest::Add)
      expect(cmd.payload.value).to eq(1)
      expect(cmd.metadata[:user_id]).to eq(10)
    end

    it 'can take a command class' do
      ctx = described_class.new(metadata: { user_id: 10 })
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd).to be_a(CccContextTest::Add)
      expect(cmd.payload.value).to eq(1)
      expect(cmd.metadata[:user_id]).to eq(10)
    end

    it 'symbolizes string keys' do
      ctx = described_class.new(metadata: { user_id: 10 })
      cmd = ctx.build('type' => 'ccc_ctest.add', 'payload' => { 'value' => 1 })
      expect(cmd).to be_a(CccContextTest::Add)
      expect(cmd.payload.value).to eq(1)
      expect(cmd.metadata[:user_id]).to eq(10)
    end

    it 'raises UnknownMessageError for unknown types' do
      ctx = described_class.new(metadata: { user_id: 10 })
      expect do
        ctx.build('type' => 'nope', 'payload' => { 'value' => 1 })
      end.to raise_error(Sourced::UnknownMessageError)
    end

    it 'raises UnknownMessageError for event types when scoped to Command' do
      ctx = described_class.new(metadata: { user_id: 10 })
      expect do
        ctx.build('type' => 'ccc_ctest.added', 'payload' => {})
      end.to raise_error(Sourced::UnknownMessageError)
    end

    it 'allows scoping to a custom command subclass' do
      custom_scope = Class.new(Sourced::CCC::Command)
      custom_cmd = custom_scope.define('ccc_ctest.custom') do
        attribute :name, String
      end

      ctx = described_class.new(metadata: { user_id: 10 }, scope: custom_scope)
      cmd = ctx.build(type: 'ccc_ctest.custom', payload: { name: 'hello' })
      expect(cmd).to be_a(custom_cmd)
      expect(cmd.payload.name).to eq('hello')
      expect(cmd.metadata[:user_id]).to eq(10)
    end
  end
end
