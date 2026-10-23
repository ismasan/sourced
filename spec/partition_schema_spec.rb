# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/testing/rspec'

module PartitionSchemaTest
  class Orders < Sourced::Decider
    partition_by :order_id, item_id: Sourced::Types::Integer

    AddItem = Command(:add_item) do
      attribute :quantity, Integer
    end

    ItemAdded = Event(:item_added) do
      attribute :quantity, Integer
    end

    command AddItem do |_context, cmd|
      event ItemAdded, cmd.payload.to_h
    end

    evolve(ItemAdded) { |*| }
  end

  class Pinned < Sourced::Decider
    consumer_group 'Some::Group'
    message_prefix 'shop.orders'
    partition_by :order_id

    Place = Command(:place)
  end

  OrderNoted = Sourced::Event.define('partition_schema_test.order_noted') do
    attribute :order_id, String
    attribute :note, String
  end

  Unkeyed = Sourced::Event.define('partition_schema_test.unkeyed') do
    attribute :note, String
  end

  IntegerOrder = Sourced::Command.define('partition_schema_test.integer_order') do
    attribute :order_id, Integer
  end

  OptionalOrder = Sourced::Command.define('partition_schema_test.optional_order') do
    attribute? :order_id, String
  end

  PresentOrder = Sourced::Command.define('partition_schema_test.present_order') do
    attribute :order_id, Sourced::Types::String.present
  end

  PlaceOrder = Sourced::Command.define('partition_schema_test.place_order') do
    attribute :order_id, String
  end
end

RSpec.describe Sourced::PartitionSchema do
  include Sourced::Testing::RSpec

  def decider(&block)
    Class.new(Sourced::Decider) do
      consumer_group 'partition-schema-test'
      partition_by :order_id
      class_eval(&block)
    end
  end

  describe 'partition_by' do
    it 'types bare keys as String, and takes types by key' do
      expect(PartitionSchemaTest::Orders.partition_schema)
        .to eq(Sourced::Types::Hash[order_id: Sourced::Types::String, item_id: Sourced::Types::Integer])
      expect(PartitionSchemaTest::Orders.partition_schema.parse(order_id: 'o1', item_id: 1)).to eq(order_id: 'o1', item_id: 1)
      expect(PartitionSchemaTest::Orders.partition_keys).to eq(%i[order_id item_id])
    end

    it 'wraps Ruby classes as types' do
      klass = Class.new(Sourced::Decider) { partition_by order_id: Integer }
      expect(klass.partition_schema).to eq(Sourced::Types::Hash[order_id: Integer])
    end

    it 'raises on a key declared twice' do
      expect { Class.new(Sourced::Decider) { partition_by :order_id, order_id: Integer } }
        .to raise_error(ArgumentError, /order_id is declared twice/)
    end

    it 'raises on a key typed by anything but a Plumb type or a class' do
      [Sourced::Types::UUID, 'String', nil].each do |type|
        expect { Class.new(Sourced::Decider) { partition_by order_id: type } }
          .to raise_error(ArgumentError, /order_id needs a Plumb type or a class, got #{Regexp.escape(type.inspect)}/)
      end
    end

    it 'is inherited' do
      sub = Class.new(PartitionSchemaTest::Orders)
      expect(sub.partition_schema).to eq(PartitionSchemaTest::Orders.partition_schema)
    end
  end

  describe 'Command and Event' do
    it 'prefix types with the group_id' do
      expect(PartitionSchemaTest::Orders::AddItem.type).to eq('partition_schema_test.orders.add_item')
      expect(PartitionSchemaTest::Orders::ItemAdded.type).to eq('partition_schema_test.orders.item_added')
      expect(PartitionSchemaTest::Orders::AddItem).to be < Sourced::Command
      expect(PartitionSchemaTest::Orders::ItemAdded).to be < Sourced::Event
    end

    it 'prefix types with a declared message_prefix' do
      expect(PartitionSchemaTest::Pinned::Place.type).to eq('shop.orders.place')
    end

    it 'declare the partition schema before the block attributes' do
      klass = PartitionSchemaTest::Orders::AddItem
      expect(klass.payload_attribute_names).to eq(%i[order_id item_id quantity])
      expect(klass.new(payload: { order_id: 'o1', item_id: 1, quantity: 2 })).to be_valid
      expect(klass.new(payload: { order_id: 'o1', item_id: 'nope', quantity: 2 })).not_to be_valid
    end

    it 'define the schema attributes without a block' do
      expect(PartitionSchemaTest::Pinned::Place.payload_attribute_names).to eq(%i[order_id])
    end

    it 'lock the schema and prefix once a message is defined' do
      klass = PartitionSchemaTest::Pinned
      expect { klass.partition_by :other }.to raise_error(ArgumentError, /declare partition_by before defining messages/)
      expect { klass.consumer_group 'other' }.to raise_error(ArgumentError, /declare consumer_group before/)
      expect { klass.message_prefix 'other' }.to raise_error(ArgumentError, /declare message_prefix before/)
    end

    it 'need a group_id or a message_prefix' do
      klass = Class.new(Sourced::Decider) { partition_by :order_id }
      expect { klass.Command(:place) }.to raise_error(ArgumentError, /needs a group_id, or declare message_prefix/)
    end
  end

  describe '.check!' do
    it 'passes messages built with the helpers' do
      expect { described_class.check!(PartitionSchemaTest::Orders) }.not_to raise_error
    end

    it 'requires commands to declare every key' do
      reactor = decider { command(PartitionSchemaTest::Unkeyed) { |*| } }
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /command PartitionSchemaTest::Unkeyed must declare order_id/)
    end

    it 'requires commands to declare keys as required attributes' do
      reactor = decider { command(PartitionSchemaTest::OptionalOrder) { |*| } }
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /OptionalOrder must declare order_id/)
    end

    it 'requires reactions to declare every key' do
      reactor = decider do
        command(PartitionSchemaTest::PlaceOrder) { |*| }
        reaction(PartitionSchemaTest::Unkeyed) { |*| }
      end
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /reaction PartitionSchemaTest::Unkeyed must declare order_id/)
    end

    it 'requires the schema type, or a stricter one' do
      reactor = decider { command(PartitionSchemaTest::IntegerOrder) { |*| } }
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /IntegerOrder declares order_id as Integer, but .* partitions by order_id as Sourced::Types::String/)

      expect { described_class.check!(decider { command(PartitionSchemaTest::PresentOrder) { |*| } }) }.not_to raise_error
    end

    it 'allows evolved events with some of the keys' do
      reactor = Class.new(Sourced::Projector::StateStored) do
        consumer_group 'partition-schema-projector'
        partition_by :order_id, item_id: Sourced::Types::Integer
        context { {} }
        evolve(PartitionSchemaTest::OrderNoted) { |*| }
        evolve(PartitionSchemaTest::Orders::ItemAdded) { |*| }
      end
      expect { described_class.check!(reactor) }.not_to raise_error
    end

    it 'checks the types of the keys an evolved event declares' do
      reactor = Class.new(Sourced::Projector::StateStored) do
        consumer_group 'partition-schema-projector'
        partition_by :order_id, item_id: Sourced::Types::Integer
        context { {} }
        evolve(PartitionSchemaTest::Orders::ItemAdded) { |*| }
        evolve(PartitionSchemaTest::IntegerOrder) { |*| }
      end
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /message PartitionSchemaTest::IntegerOrder declares order_id as Integer/)
    end

    it 'requires evolved events to declare a key' do
      reactor = decider do
        command(PartitionSchemaTest::PlaceOrder) { |*| }
        evolve(PartitionSchemaTest::Unkeyed) { |*| }
      end
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /message PartitionSchemaTest::Unkeyed declares none of the partition keys order_id/)
    end

    it 'requires a message declaring every key' do
      reactor = Class.new(Sourced::Projector::StateStored) do
        consumer_group 'partition-schema-projector'
        partition_by :order_id, :item_id
        context { {} }
        evolve(PartitionSchemaTest::OrderNoted) { |*| }
      end
      expect { described_class.check!(reactor) }
        .to raise_error(Sourced::DefinitionError, /no handled message declares every partition key order_id, item_id/)
    end

    it 'skips id-partitioned reactors and reactors without a schema' do
      queue = Class.new do
        extend Sourced::Consumer
        exclusive
        def self.handled_messages = [PartitionSchemaTest::Unkeyed]
      end
      duck = Class.new do
        def self.partition_keys = [:order_id]
        def self.handled_messages = [PartitionSchemaTest::Unkeyed]
      end
      expect { described_class.check!(queue) }.not_to raise_error
      expect { described_class.check!(duck) }.not_to raise_error
    end
  end

  it 'is checked on Router registration' do
    router = Sourced::Router.new(store: Sourced::Store.new(Sequel.sqlite))
    reactor = decider { command(PartitionSchemaTest::IntegerOrder) { |*| } }
    expect { router.register(reactor) }.to raise_error(Sourced::DefinitionError, /declares order_id as Integer/)
  end

  it 'is checked by the GWT helpers' do
    reactor = decider { command(PartitionSchemaTest::IntegerOrder) { |*| } }
    expect { with_reactor(reactor, order_id: 'o1') }.to raise_error(Sourced::DefinitionError, /declares order_id as Integer/)
  end

  describe 'machines' do
    it 'type the keys of their generated messages' do
      machine = Class.new(Sourced::Machine) do
        consumer_group 'partition_schema_test.typed_machine'
        partition_by order_id: Sourced::Types::Integer
        State(:open)
      end
      type, = Sourced::PartitionSchema.attributes_of(machine.declared_states.values.first)[:order_id]
      expect(type).to eq(Sourced::Types::Integer)
    end

    it 'check commands against the schema' do
      machine = Class.new(Sourced::Machine) do
        consumer_group 'partition_schema_test.mistyped_machine'
        partition_by :order_id
        State(:open)
        initial { command PartitionSchemaTest::IntegerOrder => :open }
        state :open
      end
      expect { machine.validate! }
        .to raise_error(Sourced::Machine::DefinitionError, /command PartitionSchemaTest::IntegerOrder declares order_id as Integer/)
    end
  end
end
