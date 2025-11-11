# frozen_string_literal: true

require 'spec_helper'
require 'sourced/injector'

RSpec.describe Sourced::Injector do
  describe '.resolve_args' do
    context 'with class method' do
      let(:target) do
        Class.new do
          def self.handle(backend:, logger:, clock: 'default-clock')
            [backend, logger, clock]
          end
        end
      end

      it 'returns a list of argument names for an object and method' do
        args = described_class.resolve_args(target, :handle)
        expect(args).to eq(%i[backend logger clock])
      end
    end

    context 'with method having only required keyword arguments' do
      let(:target) do
        Class.new do
          def self.handle(replaying:, history:)
            [replaying, history]
          end
        end
      end

      it 'returns only required keyword argument names' do
        args = described_class.resolve_args(target, :handle)
        expect(args).to eq(%i[replaying history])
      end
    end

    context 'with method having only optional keyword arguments' do
      let(:target) do
        Class.new do
          def self.handle(backend: nil, logger: 'default')
            [backend, logger]
          end
        end
      end

      it 'returns optional keyword argument names' do
        args = described_class.resolve_args(target, :handle)
        expect(args).to eq(%i[backend logger])
      end
    end

    context 'with method having no keyword arguments' do
      let(:target) do
        Class.new do
          def self.handle(event)
            event
          end
        end
      end

      it 'returns empty array' do
        args = described_class.resolve_args(target, :handle)
        expect(args).to eq([])
      end
    end

    context 'with method having mixed positional and keyword arguments' do
      let(:target) do
        Class.new do
          def self.handle(event, stream_id, replaying:, history: nil)
            [event, stream_id, replaying, history]
          end
        end
      end

      it 'returns only keyword argument names' do
        args = described_class.resolve_args(target, :handle)
        expect(args).to eq(%i[replaying history])
      end
    end

    context 'with method having keyword splat arguments' do
      let(:target) do
        Class.new do
          def self.handle(event, replaying:, **kwargs)
            [event, replaying, kwargs]
          end
        end
      end

      it 'ignores keyword splat and returns explicit keywords' do
        args = described_class.resolve_args(target, :handle)
        expect(args).to eq(%i[replaying])
      end
    end

    context 'with instance method via initialize' do
      let(:target) do
        Class.new do
          def initialize(backend:, logger: nil)
            @backend = backend
            @logger = logger
          end
        end
      end

      it 'resolves constructor arguments' do
        args = described_class.resolve_args(target, :new)
        expect(args).to eq(%i[backend logger])
      end
    end

    context 'with proc argument' do
      let(:proc_target) do
        proc { |event, replaying:, history: nil| [event, replaying, history] }
      end

      it 'resolves proc parameters' do
        args = described_class.resolve_args(proc_target)
        expect(args).to eq(%i[replaying history])
      end
    end
  end
end
