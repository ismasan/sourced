# frozen_string_literal: true

require 'spec_helper'
require 'sourced'
require 'sourced/testing/rspec'

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

  AddItem = Sourced::Command.define('evolve_test.add_item') do
    attribute :cart_id, String
    attribute :item_id, String
  end

  class CartProjector < Sourced::Projector::Inline
    context do
      { cart_id: nil, items: [] }
    end

    evolve ItemAdded do |context, evt|
      context[:cart_id] = evt.payload.cart_id
      context[:items] << evt.payload.item_id
    end

    evolve ItemRemoved do |context, evt|
      context[:items].delete(evt.payload.item_id)
    end
  end
end

RSpec.describe Sourced::Evolve do
  def added(item_id) = EvolveTestMessages::ItemAdded.new(payload: { cart_id: 'c1', item_id: })

  describe 'inline context and evolve declarations' do
    let(:decider) do
      Class.new(Sourced::Decider) do
        partition_by :cart_id

        context { { cart_id: nil, items: [] } }

        evolve EvolveTestMessages::ItemAdded do |context, evt|
          context[:cart_id] = evt.payload.cart_id
          context[:items] << evt.payload.item_id
        end

        def helper = :reactor_helper
      end
    end

    it 'evolves the reactor instance context through its projection' do
      instance = decider.new(cart_id: 'c1')
      expect(instance.evolve([added('i1')])).to eq(cart_id: 'c1', items: ['i1'])
      expect(instance.context).to eq(cart_id: 'c1', items: ['i1'])
    end

    it 'declares them on a projection subclass the reactor owns' do
      expect(decider.projector.superclass).to eq(Sourced::Decider.projector)
      expect(decider.projector).to be < Sourced::Projector::Inline
      expect(decider.history_selector.types).to eq([EvolveTestMessages::ItemAdded])
      expect(decider.new(cart_id: 'c1').projection).to be_a(decider.projector)
    end

    it "builds its selector from the projection's, by the partition keys" do
      expect(decider.history_selector).to eq(Sourced::Selector[EvolveTestMessages::ItemAdded].by(:cart_id))
    end

    it 'follows its evolve types and partition keys' do
      reactor = Class.new(Sourced::Decider) do
        partition_by :cart_id
        evolve(EvolveTestMessages::ItemAdded) { |*| }
      end
      expect(reactor.history_selector.types).to eq([EvolveTestMessages::ItemAdded])

      reactor.evolve(EvolveTestMessages::CartClosed) { |*| }
      expect(reactor.history_selector.types).to eq([EvolveTestMessages::CartClosed, EvolveTestMessages::ItemAdded].sort_by(&:type))

      reactor.partition_by :cart_id, :user_id
      expect(reactor.history_selector.keys).to eq(%i[cart_id user_id])
    end

    it 'reads no history when id-partitioned' do
      queue = Class.new(Sourced::Decider) do
        exclusive
        evolve(EvolveTestMessages::ItemAdded) { |*| }
      end
      expect(queue.history_selector).to eq(Sourced::Selector::NONE)
      expect(queue.projector.selector.types).to eq([EvolveTestMessages::ItemAdded])
    end

    it 'runs evolve blocks in the projection scope, not the reactor scope' do
      decider.evolve(EvolveTestMessages::CartClosed) { |context, _| context[:closed_by] = respond_to?(:helper) }

      instance = decider.new(cart_id: 'c1')
      instance.evolve([EvolveTestMessages::CartClosed.new(payload: { cart_id: 'c1' })])
      expect(instance.context[:closed_by]).to be(false)
    end
  end

  describe 'adopting a standalone projection' do
    let(:decider) do
      Class.new(Sourced::Decider) do
        partition_by :cart_id
        evolve EvolveTestMessages::CartProjector
      end
    end

    let(:projector) do
      Class.new(Sourced::Projector::EventSourced) do
        partition_by :cart_id
        evolve EvolveTestMessages::CartProjector
      end
    end

    it 'shares the projection between reactors' do
      [decider, projector].each do |reactor|
        expect(reactor.projector.superclass).to eq(EvolveTestMessages::CartProjector)
        expect(reactor.history_selector.types).to eq(
          [EvolveTestMessages::ItemAdded, EvolveTestMessages::ItemRemoved]
        )

        instance = reactor.new(cart_id: 'c1')
        instance.evolve([added('i1'), added('i2')])
        expect(instance.context).to eq(cart_id: 'c1', items: %w[i1 i2])
      end

      expect(projector.handled_messages).to eq([EvolveTestMessages::ItemAdded, EvolveTestMessages::ItemRemoved])
    end

    it 'extends the reactor own subclass with inline evolve blocks, leaving the shared class alone' do
      decider.evolve(EvolveTestMessages::CartClosed) { |context, _| context[:closed] = true }

      expect(decider.history_selector.types).to include(EvolveTestMessages::CartClosed)
      expect(EvolveTestMessages::CartProjector.selector.types).not_to include(EvolveTestMessages::CartClosed)
      expect(projector.history_selector.types).not_to include(EvolveTestMessages::CartClosed)
      expect(EvolveTestMessages::CartProjector.new.respond_to?(
        Sourced.message_method_name(Sourced::Projector::Inline::PREFIX, EvolveTestMessages::CartClosed.to_s)
      )).to be(false)
    end

    it 'raises after the reactor own context or evolve declarations' do
      expect {
        Class.new(Sourced::Decider) do
          context { {} }
          evolve EvolveTestMessages::CartProjector
        end
      }.to raise_error(ArgumentError, /must come before/)

      expect {
        Class.new(Sourced::Decider) do
          evolve(EvolveTestMessages::CartClosed) { |_, _| nil }
          evolve EvolveTestMessages::CartProjector
        end
      }.to raise_error(ArgumentError, /must come before/)

      expect { decider.evolve(EvolveTestMessages::CartProjector) }
        .to raise_error(ArgumentError, /must come before/)
    end

    it 'raises after the reactor own before_evolve declarations' do
      expect {
        Class.new(Sourced::Decider) do
          before_evolve { |_, _| nil }
          evolve EvolveTestMessages::CartProjector
        end
      }.to raise_error(ArgumentError, /must come before/)
    end

    it 'adds before_evolve blocks to the reactor own subclass, leaving the shared class alone' do
      decider.before_evolve { |context, _| context[:seen] = (context[:seen] || 0) + 1 }

      instance = decider.new(cart_id: 'c1')
      instance.evolve([added('i1'), added('i2')])
      expect(instance.context[:seen]).to eq(2)
      expect(EvolveTestMessages::CartProjector.before_evolve_blocks).to be_empty
    end

    it 'raises when given a block' do
      expect { Class.new(Sourced::Decider).evolve(EvolveTestMessages::CartProjector) { nil } }
        .to raise_error(ArgumentError, /takes no block/)
    end

    it 'names the projection after the reactor and the adopted class' do
      stub_const('CartDecider', decider)
      expect(decider.projector.to_s).to eq('CartDecider::Projector(EvolveTestMessages::CartProjector)')
      expect(decider.projector.inspect).to eq(decider.projector.to_s)
    end
  end

  describe 'reactor inheritance' do
    let(:parent) do
      Class.new(Sourced::Decider) do
        context { { items: [] } }
        evolve(EvolveTestMessages::ItemAdded) { |context, evt| context[:items] << evt.payload.item_id }
      end
    end

    it 'subclasses inherit the parent projection on a subclass of their own' do
      child = Class.new(parent)
      expect(child.projector.superclass).to eq(parent.projector)
      expect(child.projector.selector.types).to eq([EvolveTestMessages::ItemAdded])

      instance = child.new
      instance.evolve([added('i1')])
      expect(instance.context).to eq(items: ['i1'])
    end

    it 'declarations on a subclass do not change the parent' do
      child = Class.new(parent) do
        evolve(EvolveTestMessages::CartClosed) { |context, _| context[:closed] = true }
      end

      expect(child.projector.selector.types).to include(EvolveTestMessages::CartClosed)
      expect(parent.projector.selector.types).not_to include(EvolveTestMessages::CartClosed)
    end

    it 'subclasses can adopt a projection, replacing the inherited one' do
      child = Class.new(parent) { evolve EvolveTestMessages::CartProjector }

      expect(child.projector.selector.types).to eq([EvolveTestMessages::ItemAdded, EvolveTestMessages::ItemRemoved])
      expect(child.new(cart_id: 'c1').context).to eq(cart_id: nil, items: [])
    end
  end

  describe 'a selector source' do
    include Sourced::Testing::RSpec

    let(:selector) do
      Sourced::Selector[EvolveTestMessages::ItemAdded, EvolveTestMessages::CartClosed]
    end

    let(:decider) do
      source = selector
      Class.new(Sourced::Decider) do
        partition_by :cart_id
        evolve source

        command EvolveTestMessages::AddItem => EvolveTestMessages::ItemAdded do |history, cmd|
          item_id = "#{cmd.payload.item_id}:#{history.size}"
          event EvolveTestMessages::ItemAdded, cart_id: cmd.payload.cart_id, item_id:
        end
      end
    end

    def closed(cart_id = 'c1') = EvolveTestMessages::CartClosed.new(payload: { cart_id: })
    def add(item_id) = EvolveTestMessages::AddItem.new(payload: { cart_id: 'c1', item_id: })

    it 'is the source, as given, read in the partition' do
      expect(decider.source).to be(selector)
      expect(decider.projector).to be < Sourced::Selector::Projection
      expect(decider.projector.selector).to be(selector)
      expect(decider.evolve_selector).to eq(selector)
      expect(decider.history_selector).to eq(selector.by(:cart_id))
    end

    it 'scopes only the parts without keys of a mixed selector' do
      source = Sourced::Selector[EvolveTestMessages::CartClosed] +
               Sourced::Selector[EvolveTestMessages::ItemAdded].by(:item_id)
      decider = Class.new(Sourced::Decider) do
        partition_by :cart_id, :item_id
        evolve source
      end

      expect(decider.history_selector).to eq(
        Sourced::Selector[EvolveTestMessages::CartClosed].by(:cart_id, :item_id) +
        Sourced::Selector[EvolveTestMessages::ItemAdded].by(:item_id)
      )
    end

    it "gives the instance the messages of the selector's types as context, taking history as read" do
      removed = EvolveTestMessages::ItemRemoved.new(payload: { cart_id: 'c1', item_id: 'i1' })
      instance = decider.new(cart_id: 'c1')
      history = [added('i1'), removed, closed]

      expect(instance.evolve(history)).to eq([history[0], history[2]])
      expect(instance.context).to eq([history[0], history[2]])
    end

    it 'raises when a command emits an event for another partition' do
      other = Class.new(Sourced::Decider) do
        partition_by :cart_id
        evolve Sourced::Selector[EvolveTestMessages::ItemAdded]

        command EvolveTestMessages::AddItem => EvolveTestMessages::ItemAdded do |_, cmd|
          event EvolveTestMessages::ItemAdded, cart_id: 'c2', item_id: cmd.payload.item_id
        end
      end

      expect { other.new(cart_id: 'c1').decide(add('i1')) }
        .to raise_error(Sourced::Decider::InvalidEventError, /cart_id "c2", but .* where cart_id is "c1"/)
    end

    it 'adds what a command emits to the history the next one sees' do
      instance = decider.new(cart_id: 'c1')
      instance.evolve([added('i1')])

      instance.decide(add('i2'))
      events = instance.decide(add('i3'))

      expect(events.map { it.payload.item_id }).to eq(['i3:2'])
      expect(instance.context.map { it.payload.item_id }).to eq(%w[i1 i2:1 i3:2])
    end

    it 'decides through the reactor runtime (GWT, handle_batch)' do
      with_reactor(decider, cart_id: 'c1')
        .given(EvolveTestMessages::ItemAdded, cart_id: 'c1', item_id: 'i1')
        .when(EvolveTestMessages::AddItem, cart_id: 'c1', item_id: 'i2')
        .then(EvolveTestMessages::ItemAdded, cart_id: 'c1', item_id: 'i2:1')
    end

    it 'takes no inline declarations' do
      expect { decider.evolve(EvolveTestMessages::ItemRemoved) { |_, _| nil } }
        .to raise_error(ArgumentError, /a selector projection takes no context/)
      expect { decider.context { {} } }.to raise_error(ArgumentError, /a selector projection takes no/)
      expect { decider.before_evolve { |_, _| nil } }.to raise_error(ArgumentError, /a selector projection takes no/)
    end

    it 'is inherited as given, and can be replaced' do
      child = Class.new(decider)
      replaced = Class.new(decider) { evolve EvolveTestMessages::CartProjector }

      expect(child.source).to be(selector)
      expect(child.projector.superclass).to be(decider.projector)
      expect(replaced.source).to be(EvolveTestMessages::CartProjector)
      expect(replaced.projector.superclass).to eq(EvolveTestMessages::CartProjector)
    end
  end
end
