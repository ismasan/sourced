# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

module EvolveTestMessages
  ItemAdded = Sourced::Message.define('evolve_test.item.added') do
    attribute :item_id, String
    attribute :name, String
  end

  ItemRemoved = Sourced::Message.define('evolve_test.item.removed') do
    attribute :item_id, String
  end

  Unhandled = Sourced::Message.define('evolve_test.unhandled') do
    attribute :foo, String
  end
end

RSpec.describe Sourced::Evolve do
  let(:evolver_class) do
    Class.new do
      include Sourced::Evolve

      def initialize(partition_values = {})
        @partition_values = partition_values
      end

      state do |partition_values|
        { items: [], partition_values: partition_values }
      end

      evolve EvolveTestMessages::ItemAdded do |state, msg|
        state[:items] << { id: msg.payload.item_id, name: msg.payload.name }
      end

      evolve EvolveTestMessages::ItemRemoved do |state, msg|
        state[:items].reject! { |i| i[:id] == msg.payload.item_id }
      end
    end
  end

  describe '.state' do
    it 'initializes state with partition values hash' do
      instance = evolver_class.new(key1: 'val1', key2: 'val2')
      expect(instance.state[:partition_values]).to eq({ key1: 'val1', key2: 'val2' })
    end
  end

  describe '#evolve' do
    it 'applies registered handlers in order' do
      instance = evolver_class.new
      messages = [
        EvolveTestMessages::ItemAdded.new(payload: { item_id: 'i1', name: 'Apple' }),
        EvolveTestMessages::ItemAdded.new(payload: { item_id: 'i2', name: 'Banana' }),
        EvolveTestMessages::ItemRemoved.new(payload: { item_id: 'i1' })
      ]

      instance.evolve(messages)

      expect(instance.state[:items]).to eq([{ id: 'i2', name: 'Banana' }])
    end

    it 'skips unregistered message types' do
      instance = evolver_class.new
      messages = [
        EvolveTestMessages::ItemAdded.new(payload: { item_id: 'i1', name: 'Apple' }),
        EvolveTestMessages::Unhandled.new(payload: { foo: 'bar' })
      ]

      instance.evolve(messages)

      expect(instance.state[:items]).to eq([{ id: 'i1', name: 'Apple' }])
    end
  end

  describe '.handled_messages_for_evolve' do
    it 'tracks registered classes' do
      expect(evolver_class.handled_messages_for_evolve).to contain_exactly(
        EvolveTestMessages::ItemAdded,
        EvolveTestMessages::ItemRemoved
      )
    end
  end

  describe 'inheritance' do
    it 'subclass inherits evolve handlers' do
      subclass = Class.new(evolver_class)
      expect(subclass.handled_messages_for_evolve).to contain_exactly(
        EvolveTestMessages::ItemAdded,
        EvolveTestMessages::ItemRemoved
      )

      instance = subclass.new
      instance.evolve([
        EvolveTestMessages::ItemAdded.new(payload: { item_id: 'i1', name: 'Apple' })
      ])
      expect(instance.state[:items]).to eq([{ id: 'i1', name: 'Apple' }])
    end
  end
end
