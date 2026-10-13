# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

module EvolveTestMessages
  ItemAdded = Sourced::Message.define('evolve_test.item.added') do
    attribute :cart_id, String
    attribute :item_id, String
  end

  ItemRemoved = Sourced::Message.define('evolve_test.item.removed') do
    attribute :cart_id, String
    attribute :item_id, String
  end

  CartClosed = Sourced::Message.define('evolve_test.cart.closed') do
    attribute :cart_id, String
  end

  class CartProjection < Sourced::Projection
    state do
      { cart_id: nil, items: [] }
    end

    evolve ItemAdded do |state, evt|
      state[:cart_id] = evt.payload.cart_id
      state[:items] << evt.payload.item_id
    end

    evolve ItemRemoved do |state, evt|
      state[:items].delete(evt.payload.item_id)
    end
  end
end

RSpec.describe Sourced::Evolve do
  def added(item_id) = EvolveTestMessages::ItemAdded.new(payload: { cart_id: 'c1', item_id: })

  describe 'inline state and evolve declarations' do
    let(:decider) do
      Class.new(Sourced::Decider) do
        partition_by :cart_id

        state { { cart_id: nil, items: [] } }

        evolve EvolveTestMessages::ItemAdded do |state, evt|
          state[:cart_id] = evt.payload.cart_id
          state[:items] << evt.payload.item_id
        end

        def helper = :reactor_helper
      end
    end

    it 'evolves the reactor instance state through its projection' do
      instance = decider.new(cart_id: 'c1')
      expect(instance.evolve([added('i1')])).to eq(cart_id: 'c1', items: ['i1'])
      expect(instance.state).to eq(cart_id: 'c1', items: ['i1'])
    end

    it 'declares them on a projection subclass the reactor owns' do
      expect(decider.projection_class.superclass).to eq(Sourced::Decider.projection_class)
      expect(decider.projection_class).to be < Sourced::Projection
      expect(decider.handled_messages_for_evolve).to eq([EvolveTestMessages::ItemAdded])
      expect(decider.new(cart_id: 'c1').projection).to be_a(decider.projection_class)
    end

    it 'builds history conditions from the projection types' do
      types = decider.context_for(cart_id: 'c1').map(&:message_type).uniq
      expect(types).to eq([EvolveTestMessages::ItemAdded.type])
    end

    it 'runs evolve blocks in the projection scope, not the reactor scope' do
      decider.evolve(EvolveTestMessages::CartClosed) { |state, _| state[:closed_by] = respond_to?(:helper) }

      instance = decider.new(cart_id: 'c1')
      instance.evolve([EvolveTestMessages::CartClosed.new(payload: { cart_id: 'c1' })])
      expect(instance.state[:closed_by]).to be(false)
    end
  end

  describe 'adopting a standalone projection' do
    let(:decider) do
      Class.new(Sourced::Decider) do
        partition_by :cart_id
        evolve EvolveTestMessages::CartProjection
      end
    end

    let(:projector) do
      Class.new(Sourced::Projector::EventSourced) do
        partition_by :cart_id
        evolve EvolveTestMessages::CartProjection
      end
    end

    it 'shares the projection between reactors' do
      [decider, projector].each do |reactor|
        expect(reactor.projection_class.superclass).to eq(EvolveTestMessages::CartProjection)
        expect(reactor.handled_messages_for_evolve).to eq(
          [EvolveTestMessages::ItemAdded, EvolveTestMessages::ItemRemoved]
        )

        instance = reactor.new(cart_id: 'c1')
        instance.evolve([added('i1'), added('i2')])
        expect(instance.state).to eq(cart_id: 'c1', items: %w[i1 i2])
      end

      expect(projector.handled_messages).to eq([EvolveTestMessages::ItemAdded, EvolveTestMessages::ItemRemoved])
    end

    it 'extends the reactor own subclass with inline evolve blocks, leaving the shared class alone' do
      decider.evolve(EvolveTestMessages::CartClosed) { |state, _| state[:closed] = true }

      expect(decider.handled_messages_for_evolve).to include(EvolveTestMessages::CartClosed)
      expect(EvolveTestMessages::CartProjection.handled_messages).not_to include(EvolveTestMessages::CartClosed)
      expect(projector.handled_messages_for_evolve).not_to include(EvolveTestMessages::CartClosed)
      expect(EvolveTestMessages::CartProjection.new.respond_to?(
        Sourced.message_method_name(Sourced::Projection::PREFIX, EvolveTestMessages::CartClosed.to_s)
      )).to be(false)
    end

    it 'raises after the reactor own state or evolve declarations' do
      expect {
        Class.new(Sourced::Decider) do
          state { {} }
          evolve EvolveTestMessages::CartProjection
        end
      }.to raise_error(ArgumentError, /must come before/)

      expect {
        Class.new(Sourced::Decider) do
          evolve(EvolveTestMessages::CartClosed) { |_, _| nil }
          evolve EvolveTestMessages::CartProjection
        end
      }.to raise_error(ArgumentError, /must come before/)

      expect { decider.evolve(EvolveTestMessages::CartProjection) }
        .to raise_error(ArgumentError, /must come before/)
    end

    it 'raises when given a block' do
      expect { Class.new(Sourced::Decider).evolve(EvolveTestMessages::CartProjection) { nil } }
        .to raise_error(ArgumentError, /takes no block/)
    end

    it 'names the projection after the reactor and the adopted class' do
      stub_const('CartDecider', decider)
      expect(decider.projection_class.to_s).to eq('CartDecider::Projection(EvolveTestMessages::CartProjection)')
      expect(decider.projection_class.inspect).to eq(decider.projection_class.to_s)
    end
  end

  describe 'reactor inheritance' do
    let(:parent) do
      Class.new(Sourced::Decider) do
        state { { items: [] } }
        evolve(EvolveTestMessages::ItemAdded) { |state, evt| state[:items] << evt.payload.item_id }
      end
    end

    it 'subclasses inherit the parent projection on a subclass of their own' do
      child = Class.new(parent)
      expect(child.projection_class.superclass).to eq(parent.projection_class)
      expect(child.handled_messages_for_evolve).to eq([EvolveTestMessages::ItemAdded])

      instance = child.new
      instance.evolve([added('i1')])
      expect(instance.state).to eq(items: ['i1'])
    end

    it 'declarations on a subclass do not change the parent' do
      child = Class.new(parent) do
        evolve(EvolveTestMessages::CartClosed) { |state, _| state[:closed] = true }
      end

      expect(child.handled_messages_for_evolve).to include(EvolveTestMessages::CartClosed)
      expect(parent.handled_messages_for_evolve).not_to include(EvolveTestMessages::CartClosed)
    end

    it 'subclasses can adopt a projection, replacing the inherited one' do
      child = Class.new(parent) { evolve EvolveTestMessages::CartProjection }

      expect(child.handled_messages_for_evolve).to eq([EvolveTestMessages::ItemAdded, EvolveTestMessages::ItemRemoved])
      expect(child.new(cart_id: 'c1').state).to eq(cart_id: nil, items: [])
    end
  end
end
