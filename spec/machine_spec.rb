# frozen_string_literal: true

require 'spec_helper'
require_relative './support/test_domain'

RSpec.describe Sors::Machine do
  specify 'Router.reactors_for' do
    evt = TestDomain::Carts::ItemAdded.new(stram_id: '1', data: { item_id: '1', quantity: 1 })
    expect(Sors::Router.reactors_for([evt])).to eq([TestDomain::Carts.new])
  end

  specify 'handling commands, producing events, scheduling reactors' do
    cmd = TestDomain::Carts::AddItem.parse(
      stream_id: 'cart-1',
      payload: { product_id: 1, quantity: 2 }
    )
    cart, events = Sors::Router.handle(cmd)
    expect(cart.total).to eq(200)
    expect(events.size).to eq(1)
    expect(events.first).to be_a(TestDomain::Carts::ItemAdded)
    expect(events.first.causation_id).to eq(cmd.id)
    expect(events.first.payload.product_id).to eq(1)
    expect(events.first.payload.product_name).to eq('Apple')

    # Test that initial events were appended to the store synchronously
    events = Sors.config.backend.read_event_stream('cart-1')
    expect(events.map(&:class)).to eq([
                                        TestDomain::Carts::AddItem,
                                        TestDomain::Carts::ItemAdded
                                      ])

    # Run reactors synchronously and test that they produce new events
    # Normally these reactors run in background fibers or processes
    # (or both)
    Sors::Worker.drain

    # debugger
    events = Sors.config.backend.read_event_stream('cart-1')
    expect(events.map(&:class)).to eq([
                                        TestDomain::Carts::AddItem,
                                        TestDomain::Carts::ItemAdded,
                                        TestDomain::Carts::SendEmail,
                                        TestDomain::Carts::EmailSent,
                                        TestDomain::Carts::PlaceOrder,
                                        TestDomain::Carts::OrderPlaced
                                      ])
  end
end
