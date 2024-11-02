# frozen_string_literal: true

require 'spec_helper'
require_relative './support/test_domain'

RSpec.describe Sors::Machine do
  before do
    Sors.config.backend.clear!
  end

  specify 'handling commands, producing events, scheduling reactors' do
    cmd = TestDomain::Carts::AddItem.parse(
      stream_id: 'cart-1',
      payload: { product_id: 1, quantity: 2 }
    )
    cart, events = Sors::Router.handle(cmd)
    expect(cart.total).to eq(200)
    expect(cart.status).to eq(:open)
    expect(events.size).to eq(3)
    expect(events[0]).to be_a(TestDomain::Carts::AddItem)
    expect(events[1]).to be_a(TestDomain::Carts::CartStarted)
    expect(events[2]).to be_a(TestDomain::Carts::ItemAdded)
    expect(events[1].causation_id).to eq(cmd.id)
    expect(events[2].payload.product_id).to eq(1)
    expect(events[2].payload.product_name).to eq('Apple')
    expect(events.map(&:seq)).to eq([1, 2, 3])

    # Test that initial events were appended to the store synchronously
    events = Sors.config.backend.read_event_stream('cart-1')
    expect(events.map(&:class)).to eq([
      TestDomain::Carts::AddItem,
      TestDomain::Carts::CartStarted,
      TestDomain::Carts::ItemAdded
    ])

    # Run reactors synchronously and test that they produce new events
    # Normally these reactors run in background fibers or processes
    # (or both)
    Sors::Worker.drain

    events = Sors.config.backend.read_event_stream('cart-1')
    expect(events.map(&:class)).to eq([
      TestDomain::Carts::AddItem,
      TestDomain::Carts::CartStarted,
      TestDomain::Carts::ItemAdded,
      TestDomain::Carts::SendItemAddedWebhook,
      TestDomain::Carts::ItemAddedWebhookSent
    ])

    cart = TestDomain::Carts.replay('cart-1')
    expect(cart.webhooks_sent).to eq(1)
  end

  specify 'inheritance' do
    machine = Class.new(TestDomain::Carts) do
      decide TestDomain::Carts::RandomCommand do |_, _|
        []
      end

      evolve TestDomain::Carts::RandomEvent do |_, _|
      end

      react TestDomain::Carts::RandomEvent do
      end
    end

    expect(machine.handled_commands).to eq([*TestDomain::Carts.handled_commands, TestDomain::Carts::RandomCommand])
    expect(machine.handled_events_for_evolve).to eq([
      *TestDomain::Carts.handled_events_for_evolve,
      TestDomain::Carts::RandomEvent
    ])
    expect(machine.handled_events_for_react).to eq([
      *TestDomain::Carts.handled_events_for_react,
      TestDomain::Carts::RandomEvent
    ])
  end
end
