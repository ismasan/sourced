# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

module CccContextTest
  Add = Sourced::CCC::Command.define('ccc_ctest.add') do
    attribute :value, Integer
  end

  Remove = Sourced::CCC::Command.define('ccc_ctest.remove') do
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

  describe 'callback hooks' do
    it 'runs on block for matching command type' do
      klass = Class.new(described_class)
      klass.on(CccContextTest::Add) { |_app, cmd| cmd.with_payload(value: cmd.payload.value + 10) }

      ctx = klass.new
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.payload.value).to eq(11)
    end

    it 'does not run on block for non-matching command type' do
      klass = Class.new(described_class)
      klass.on(CccContextTest::Add) { |_app, cmd| cmd.with_payload(value: cmd.payload.value + 10) }

      ctx = klass.new
      cmd = ctx.build(CccContextTest::Remove, payload: { value: 1 })
      expect(cmd.payload.value).to eq(1)
    end

    it 'passes app scope to on block' do
      app = double('app', session_id: 'abc')
      klass = Class.new(described_class)
      klass.on(CccContextTest::Add) { |a, cmd| cmd.with_metadata(session_id: a.session_id) }

      ctx = klass.new(app: app)
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.metadata[:session_id]).to eq('abc')
    end

    it 'runs any block for all commands' do
      klass = Class.new(described_class)
      klass.any { |_app, cmd| cmd.with_metadata(source: 'web') }

      ctx = klass.new
      add_cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      remove_cmd = ctx.build(CccContextTest::Remove, payload: { value: 2 })
      expect(add_cmd.metadata[:source]).to eq('web')
      expect(remove_cmd.metadata[:source]).to eq('web')
    end

    it 'runs on before any (pipeline order)' do
      klass = Class.new(described_class)
      klass.on(CccContextTest::Add) { |_app, cmd| cmd.with_metadata(step: 'on') }
      klass.any { |_app, cmd| cmd.with_metadata(step: "#{cmd.metadata[:step]}_any") }

      ctx = klass.new
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.metadata[:step]).to eq('on_any')
    end

    it 'runs multiple any blocks in order' do
      klass = Class.new(described_class)
      klass.any { |_app, cmd| cmd.with_metadata(steps: ['first']) }
      klass.any { |_app, cmd| cmd.with_metadata(steps: cmd.metadata[:steps] + ['second']) }

      ctx = klass.new
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.metadata[:steps]).to eq(%w[first second])
    end

    it 'passes through unchanged when no hooks registered' do
      ctx = described_class.new(metadata: { user_id: 10 })
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.payload.value).to eq(1)
      expect(cmd.metadata[:user_id]).to eq(10)
    end

    it 'app defaults to nil' do
      klass = Class.new(described_class)
      received_app = :not_set
      klass.any { |a, cmd| received_app = a; cmd }

      ctx = klass.new
      ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(received_app).to be_nil
    end

    it 'subclass inherits parent blocks' do
      parent = Class.new(described_class)
      parent.on(CccContextTest::Add) { |_app, cmd| cmd.with_payload(value: cmd.payload.value + 100) }
      parent.any { |_app, cmd| cmd.with_metadata(inherited: true) }

      child = Class.new(parent)

      ctx = child.new
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.payload.value).to eq(101)
      expect(cmd.metadata[:inherited]).to eq(true)
    end

    it 'subclass blocks do not affect parent' do
      parent = Class.new(described_class)
      child = Class.new(parent)
      child.any { |_app, cmd| cmd.with_metadata(child_only: true) }

      ctx = parent.new
      cmd = ctx.build(CccContextTest::Add, payload: { value: 1 })
      expect(cmd.metadata).not_to have_key(:child_only)
    end

    it 'works with build from type string + on block' do
      klass = Class.new(described_class)
      klass.on(CccContextTest::Add) { |_app, cmd| cmd.with_payload(value: cmd.payload.value + 5) }

      ctx = klass.new
      cmd = ctx.build(type: 'ccc_ctest.add', payload: { value: 1 })
      expect(cmd.payload.value).to eq(6)
    end
  end
end
