# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Sync do
  let(:host) do
    Class.new do
      include Sourced::Sync

      attr_reader :calls1, :calls2

      def initialize
        @calls1 = []
        @calls2 = []
      end

      sync do |name, age|
        @calls1 << [name, age]
      end

      sync do |_name, age|
        @calls2 << age + 2
      end
    end
  end

  context 'with Procs' do
    it 'returns sync blocks bound to host instance and arguments' do
      object = host.new
      blocks = object.sync_blocks_with('Joe', 30)
      blocks.each(&:call)
      expect(object.calls1).to eq([['Joe', 30]])
      expect(object.calls2).to eq([32])
    end
  end

  context 'with custom #call interfaces' do
    it 'returns sync blocks bound to passed arguments' do
      host = Class.new do
        include Sourced::Sync
      end

      collaborator = Struct.new(:args) do
        def call(*args)
          self.args = args
        end
      end

      synccer = collaborator.new(nil)

      host.sync synccer

      object = host.new
      blocks = object.sync_blocks_with('Joe', 30)
      blocks.each(&:call)
      expect(synccer.args).to eq(['Joe', 30])
    end
  end
end
