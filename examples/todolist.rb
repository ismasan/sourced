# frozen_string_literal: true

require 'bundler'
Bundler.setup(:test)

require 'sors'
require 'sequel'
require_relative '../spec/support/test_aggregate'

# ActiveRecord::Base.establish_connection(adapter: 'postgresql', database: 'decider')
unless ENV['backend_configured']
  puts 'aggregate config'
  Sors.configure do |config|
    config.backend = Sequel.postgres('decider')
  end
  ENV['backend_configured'] = 'true'
end

# A cart Actor/Aggregate
# Example:
#   cart = Cart.build('cart-1')
#   cart.add_item(name: 'item1', price: 100)
#   cart.place
#   cart.events
#
# The above sends a Cart::Place command
# which produces a Cart::Placed event
class Cart < Sors::Aggregate
  attr_reader :status, :notified, :items, :mailer_id

  def setup(_id)
    @status = :open
    @notified = false
    @items = []
    @mailer_id = nil
  end

  def total
    items.sum(&:price)
  end

  ItemAdded = Sors::Message.define('cart.item_added') do
    attribute :name, String
    attribute :price, Integer
  end

  Placed = Sors::Message.define('cart.placed')
  Notified = Sors::Message.define('cart.notified') do
    attribute :mailer_id, String
  end

  # Defines a Cart::AddItem command struct
  command :add_item, 'cart.add_item', name: String, price: Integer do |cmd|
    cmd.follow(ItemAdded, cmd.payload.to_h)
  end

  # Defines a Cart::Place command struct
  command :place, 'cart.place' do |cmd|
    cmd.follow(Placed)
  end

  # Defines a Cart::Notify command struct
  command :notify, 'cart.notify', mailer_id: String do |cmd|
    cmd.follow(Notified, mailer_id: cmd.payload.mailer_id)
  end

  evolve ItemAdded do |event|
    @items << event.payload
  end

  evolve Placed do |_event|
    @status = :placed
  end

  evolve Notified do |event|
    @notified = true
    @mailer_id = event.payload.mailer_id
  end
end

class Mailer < Sors::Aggregate
  EmailSent = Sors::Message.define('mailer.email_sent') do
    attribute :cart_id, String
  end

  attr_reader :sent

  def setup(_id)
    @sent = []
  end

  command :send_email, 'mailer.send_email', cart_id: String do |cmd|
    # Send email here, emit EmailSent if successful
    cmd.follow(EmailSent, cart_id: cmd.payload.cart_id)
  end

  evolve EmailSent do |event|
    @sent << event
  end
end

# A Saga that orchestrates the flow between Cart and Mailer
class CartEmailsSaga < Sors::Machine
  # Listen for Cart::Placed events and
  # send command to Mailer
  react Cart::Placed do |event|
    event.follow_with_stream_id(
      Mailer::SendEmail,
      "mailer-#{event.stream_id}",
      cart_id: event.stream_id
    )
  end

  # Listen for Mailer::EmailSent events and
  # send command to Cart
  react Mailer::EmailSent do |event|
    event.follow_with_stream_id(
      Cart::Notify,
      event.payload.cart_id,
      mailer_id: event.stream_id
    )
  end
end

Sors::Router.register(Cart)
Sors::Router.register(Mailer)
Sors::Router.register(CartEmailsSaga)
