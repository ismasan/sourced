# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

module CCCEvolveTestMessages
  ItemAdded = Sourced::CCC::Message.define('evolve_test.item.added') do
    attribute :item_id, String
    attribute :name, String
  end

  ItemRemoved = Sourced::CCC::Message.define('evolve_test.item.removed') do
    attribute :item_id, String
  end

  Unhandled = Sourced::CCC::Message.define('evolve_test.unhandled') do
    attribute :foo, String
  end
end

RSpec.describe Sourced::CCC::Evolve do
  let(:evolver_class) do
    Class.new do
      include Sourced::CCC::Evolve

      state do |partition_values|
        { items: [], partition_values: partition_values }
      end

      evolve CCCEvolveTestMessages::ItemAdded do |state, msg|
        state[:items] << { id: msg.payload.item_id, name: msg.payload.name }
      end

      evolve CCCEvolveTestMessages::ItemRemoved do |state, msg|
        state[:items].reject! { |i| i[:id] == msg.payload.item_id }
      end
    end
  end

  describe '.state' do
    it 'initializes state with partition values array' do
      instance = evolver_class.new
      instance.instance_variable_set(:@partition_values, ['val1', 'val2'])
      expect(instance.state[:partition_values]).to eq(['val1', 'val2'])
    end
  end

  describe '#evolve' do
    it 'applies registered handlers in order' do
      instance = evolver_class.new
      messages = [
        CCCEvolveTestMessages::ItemAdded.new(payload: { item_id: 'i1', name: 'Apple' }),
        CCCEvolveTestMessages::ItemAdded.new(payload: { item_id: 'i2', name: 'Banana' }),
        CCCEvolveTestMessages::ItemRemoved.new(payload: { item_id: 'i1' })
      ]

      instance.evolve(messages)

      expect(instance.state[:items]).to eq([{ id: 'i2', name: 'Banana' }])
    end

    it 'skips unregistered message types' do
      instance = evolver_class.new
      messages = [
        CCCEvolveTestMessages::ItemAdded.new(payload: { item_id: 'i1', name: 'Apple' }),
        CCCEvolveTestMessages::Unhandled.new(payload: { foo: 'bar' })
      ]

      instance.evolve(messages)

      expect(instance.state[:items]).to eq([{ id: 'i1', name: 'Apple' }])
    end
  end

  describe '.handled_messages_for_evolve' do
    it 'tracks registered classes' do
      expect(evolver_class.handled_messages_for_evolve).to contain_exactly(
        CCCEvolveTestMessages::ItemAdded,
        CCCEvolveTestMessages::ItemRemoved
      )
    end
  end

  describe 'inheritance' do
    it 'subclass inherits evolve handlers' do
      subclass = Class.new(evolver_class)
      expect(subclass.handled_messages_for_evolve).to contain_exactly(
        CCCEvolveTestMessages::ItemAdded,
        CCCEvolveTestMessages::ItemRemoved
      )

      instance = subclass.new
      instance.evolve([
        CCCEvolveTestMessages::ItemAdded.new(payload: { item_id: 'i1', name: 'Apple' })
      ])
      expect(instance.state[:items]).to eq([{ id: 'i1', name: 'Apple' }])
    end
  end
end
