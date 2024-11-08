# frozen_string_literal: true

require 'bundler'
Bundler.setup(:test)

require 'sors'
require 'sequel'

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
#   cart = Cart.new('cart-1')
#   cart.add_item(name: 'item1', price: 100)
#   cart.place
#   cart.events
#
# The above sends a Cart::Place command
# which produces a Cart::Placed event
class Cart < Sors::Decider
  State = Struct.new(:status, :notified, :items, :mailer_id) do
    def total = items.sum(:price)
  end

  def init_state(_id)
    State.new(:open, false, [], nil)
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
  command :add_item, 'cart.add_item', name: String, price: Integer do |cart, cmd|
    apply(ItemAdded, cmd.payload.to_h)
  end

  # Defines a Cart::Place command struct
  command :place, 'cart.place' do |_, cmd|
    apply(Placed)
  end

  # Defines a Cart::Notify command struct
  command :notify, 'cart.notify', mailer_id: String do |_, cmd|
    puts "#{self.class.name} #{cmd.stream_id} NOTIFY"
    apply(Notified, mailer_id: cmd.payload.mailer_id)
  end

  evolve ItemAdded do |cart, event|
    cart.items << event.payload
  end

  evolve Placed do |cart, _event|
    cart.status = :placed
  end

  evolve Notified do |cart, event|
    cart.notified = true
    cart.mailer_id = event.payload.mailer_id
  end

  # This block will run
  # in the same transaction as appending
  # new events to the store.
  # So if either fails, eveything is rolled back.
  # ergo, strong consistency.
  sync do |command, events|
    puts "#{self.class.name} #{events.last.seq} SYNC"
  end

  # Or register a Reactor interface to react to events
  # synchronously
  # sync CartListings
end

class Mailer < Sors::Decider
  EmailSent = Sors::Message.define('mailer.email_sent') do
    attribute :cart_id, String
  end

  def init_state(_id)
    []
  end

  command :send_email, 'mailer.send_email', cart_id: String do |_, cmd|
    # Send email here, emit EmailSent if successful
    apply(EmailSent, cart_id: cmd.payload.cart_id)
  end

  evolve EmailSent do |list, event|
    list << event
  end
end

# A Saga that orchestrates the flow between Cart and Mailer
class CartEmailsSaga < Sors::Decider
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

# A projector
# "reacts" to events registered with .evolve
class CartListings < Sors::Decider
  class << self
    def handled_events = self.handled_events_for_evolve

    # The Reactor interface
    # @param events [Array<Message>]
    def handle_events(events)
      # For this type of event sourced projections
      # that load current state from events
      # then apply "new" events
      # TODO: the current state already includes
      # the new events, so we need to load upto events.first.seq
      instance = load(events.first.stream_id, upto: events.first.seq - 1)
      instance.handle_events(events)
    end
  end

  def handle_events(events)
    evolve(state, events)
    save
    [] # no commands
  end

  def initialize(id, **_args)
    super
    FileUtils.mkdir_p('examples/carts')
    @path = "./examples/carts/#{id}.json"
  end

  private def save
    backend.transaction do
      run_sync_blocks(state, nil, [])
    end
  end

  def init_state(id)
    { id:, items: [], status: :open, seq: 0, seqs: [] }
  end

  sync do |cart, _command, _events|
    File.write(@path, JSON.pretty_generate(cart))
  end

  # Register all events from Cart
  # So that before_evolve runs before all cart events
  evolve_all Cart.handled_commands
  evolve_all Cart

  before_evolve do |cart, event|
    cart[:seq] = event.seq
    cart[:seqs] << event.seq
  end

  evolve Cart::Placed do |cart, event|
    cart[:status] = :placed
  end

  evolve Cart::ItemAdded do |cart, event|
    cart[:items] << event.payload.to_h
  end
end

class LoggingReactor
  extend Sors::Consumer

  class << self
    # Register as a Reactor that cares about these events
    # The workers will use this to fetch the right events
    # and ACK offsets after processing
    #
    # @return [Array<Message>]
    def handled_events = [Cart::Placed, Cart::ItemAdded]

    # Workers pass available events to this method
    # in order, with exactly-once semantics
    # If a list of commands is returned,
    # workers will send them to the router
    # to be dispatched to the appropriate command handlers.
    #
    # @param events [Array<Message>]
    # @return [Array<Message]
    def handle_events(events)
      puts "LoggingReactor received #{events}"
      []
    end
  end
end

# Cart.sync CartListings

# Register Reactor interfaces with the Router
# This allows the Router to route commands and events to reactors
Sors::Router.register(LoggingReactor)
Sors::Router.register(Cart)
Sors::Router.register(Mailer)
Sors::Router.register(CartEmailsSaga)
Sors::Router.register(CartListings)
