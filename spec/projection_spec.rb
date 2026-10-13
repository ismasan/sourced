# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

module ProjectionTestMessages
  ItemAdded = Sourced::Message.define('projection_test.item.added') do
    attribute :item_id, String
    attribute :name, String
  end

  ItemRemoved = Sourced::Message.define('projection_test.item.removed') do
    attribute :item_id, String
  end

  Unhandled = Sourced::Message.define('projection_test.unhandled') do
    attribute :foo, String
  end
end

RSpec.describe Sourced::Projection do
  let(:projection_class) do
    Class.new(Sourced::Projection) do
      state do
        { items: [] }
      end

      evolve ProjectionTestMessages::ItemAdded do |state, msg|
        state[:items] << { id: msg.payload.item_id, name: msg.payload.name }
      end

      evolve ProjectionTestMessages::ItemRemoved do |state, msg|
        state[:items].reject! { |i| i[:id] == msg.payload.item_id }
      end
    end
  end

  def added(id, name) = ProjectionTestMessages::ItemAdded.new(payload: { item_id: id, name: name })

  describe '#state' do
    it 'builds the initial state from the state block' do
      expect(projection_class.new.state).to eq(items: [])
    end

    it 'builds a fresh state per instance' do
      first = projection_class.new
      first.evolve([added('i1', 'Apple')])
      expect(projection_class.new.state).to eq(items: [])
    end

    it 'is nil without a state block' do
      expect(Class.new(Sourced::Projection).new.state).to be_nil
    end

    it 'starts from a given state instead of the state block' do
      projection = projection_class.new({ items: [{ id: 'i0', name: 'Pear' }] })
      projection.evolve([added('i1', 'Apple')])
      expect(projection.state[:items].map { |i| i[:id] }).to eq(%w[i0 i1])
    end

    it 'keeps an explicit nil starting state' do
      expect(projection_class.new(nil).state).to be_nil
    end
  end

  describe '#evolve' do
    it 'applies registered handlers in order and returns the state' do
      projection = projection_class.new
      result = projection.evolve([
        added('i1', 'Apple'),
        added('i2', 'Banana'),
        ProjectionTestMessages::ItemRemoved.new(payload: { item_id: 'i1' })
      ])

      expect(result).to equal(projection.state)
      expect(projection.state[:items]).to eq([{ id: 'i2', name: 'Banana' }])
    end

    it 'accepts a single message' do
      projection = projection_class.new
      projection.evolve(added('i1', 'Apple'))
      expect(projection.state[:items].size).to eq(1)
    end

    it 'skips unregistered message types' do
      projection = projection_class.new
      projection.evolve([added('i1', 'Apple'), ProjectionTestMessages::Unhandled.new(payload: { foo: 'bar' })])
      expect(projection.state[:items]).to eq([{ id: 'i1', name: 'Apple' }])
    end

    it 'runs evolve blocks in the projection scope' do
      klass = Class.new(Sourced::Projection) do
        state { {} }
        evolve(ProjectionTestMessages::ItemAdded) { |state, _| state[:scope] = self }
      end
      projection = klass.new
      projection.evolve([added('i1', 'Apple')])
      expect(projection.state[:scope]).to equal(projection)
    end
  end

  describe '.evolve' do
    it 'tracks registered classes once' do
      projection_class.evolve(ProjectionTestMessages::ItemAdded) { |_, _| nil }
      expect(projection_class.handled_messages).to eq([
        ProjectionTestMessages::ItemAdded,
        ProjectionTestMessages::ItemRemoved
      ])
    end

    it 'exposes them as handled_messages_for_evolve too' do
      expect(projection_class.handled_messages_for_evolve).to equal(projection_class.handled_messages)
    end

    it 'rejects state blocks that take arguments' do
      expect { Class.new(Sourced::Projection) { state { |values| values } } }
        .to raise_error(ArgumentError, /no arguments/)
      expect { Class.new(Sourced::Projection) { state { |_| {} } } }
        .to raise_error(ArgumentError, /no arguments/)
    end

    it 'rejects anything but a message class' do
      expect { projection_class.evolve(:projection_test_item_added) { nil } }
        .to raise_error(ArgumentError, /Sourced::Message subclass/)
      expect { projection_class.evolve(String) { nil } }
        .to raise_error(ArgumentError, /Sourced::Message subclass/)
    end
  end

  describe '.before_evolve' do
    let(:stamped) do
      Class.new(projection_class) do
        before_evolve { |state, msg| (state[:log] ||= []) << [:first, msg.class, state[:items].size] }
        before_evolve { |state, _| state[:log] << [:second, self.class] }
      end
    end

    it 'runs each block in order, in the projection scope, before the handler' do
      projection = stamped.new
      projection.evolve([added('i1', 'Apple')])

      expect(projection.state[:log]).to eq([
        [:first, ProjectionTestMessages::ItemAdded, 0],
        [:second, stamped]
      ])
      expect(projection.state[:items].size).to eq(1)
    end

    it 'skips messages without a handler' do
      projection = stamped.new
      projection.evolve([ProjectionTestMessages::Unhandled.new(payload: { foo: 'bar' })])
      expect(projection.state).to eq(items: [])
    end

    it 'is inherited, and blocks added to a subclass do not change the parent' do
      child = Class.new(stamped) { before_evolve { |state, _| state[:log] << :child } }

      expect(child.before_evolve_blocks.size).to eq(3)
      expect(stamped.before_evolve_blocks.size).to eq(2)
      expect(projection_class.before_evolve_blocks).to be_empty
    end

    it 'requires a block' do
      expect { projection_class.before_evolve }.to raise_error(ArgumentError, /requires a block/)
    end
  end

  describe 'inheritance' do
    it 'subclasses inherit the state block and handlers' do
      subclass = Class.new(projection_class)
      expect(subclass.handled_messages).to eq(projection_class.handled_messages)

      projection = subclass.new
      projection.evolve([added('i1', 'Apple')])
      expect(projection.state[:items]).to eq([{ id: 'i1', name: 'Apple' }])
    end

    it 'handlers added to a subclass do not change the parent' do
      subclass = Class.new(projection_class) do
        evolve(ProjectionTestMessages::Unhandled) { |state, _| state[:seen] = true }
      end

      expect(subclass.handled_messages).to include(ProjectionTestMessages::Unhandled)
      expect(projection_class.handled_messages).not_to include(ProjectionTestMessages::Unhandled)
    end
  end
end
