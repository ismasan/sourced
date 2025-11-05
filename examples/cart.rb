# frozen_string_literal: true

require 'bundler'
Bundler.setup(:test)

require 'sourced'
require 'sequel'

# ActiveRecord::Base.establish_connection(adapter: 'postgresql', database: 'decider')
unless ENV['backend_configured']
  puts 'aggregate config'
  Sourced.configure do |config|
    config.backend = Sequel.postgres('sourced_development')
  end
  Sourced.config.backend.install
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
class Cart < Sourced::Actor
  State = Struct.new(:status, :notified, :items, :mailer_id) do
    def total = items.sum(&:price)
  end

  state do |id|
    State.new(:open, false, [], nil)
  end

  ItemAdded = Sourced::Message.define('cart.item_added') do
    attribute :name, String
    attribute :price, Integer
  end

  Placed = Sourced::Message.define('cart.placed')
  Notified = Sourced::Message.define('cart.notified') do
    attribute :mailer_id, String
  end

  # Defines a Cart::AddItem command struct
  command :add_item, name: String, price: Integer do |cart, cmd|
    event(ItemAdded, cmd.payload.to_h)
  end

  # Defines a Cart::Place command struct
  command :place do |_, cmd|
    event(Placed)
  end

  # Defines a Cart::Notify command struct
  command :notify, mailer_id: String do |_, cmd|
    puts "#{self.class.name} #{cmd.stream_id} NOTIFY"
    event(Notified, mailer_id: cmd.payload.mailer_id)
  end

  def self.on_exception(exception, _message, group)
    if group.error_context[:retry_count] < 3
      later = 5 + 5 * group.error_context[:retry_count]
      group.retry(later)
    else
      group.stop(exception)
    end
  end

  event ItemAdded do |cart, event|
    cart.items << event.payload
  end

  event Placed do |cart, _event|
    cart.status = :placed
  end

  event Notified do |cart, event|
    cart.notified = true
    cart.mailer_id = event.payload.mailer_id
  end

  # This block will run
  # in the same transaction as appending
  # new events to the store.
  # So if either fails, eveything is rolled back.
  # ergo, strong consistency.
  sync do |command:, events:, state:|
    puts "#{self.class.name} #{events.last.seq} SYNC"
  end

  # Or register a Reactor interface to react to events
  # synchronously
  # sync CartListings
end

class Mailer < Sourced::Actor
  EmailSent = Sourced::Message.define('mailer.email_sent') do
    attribute :cart_id, String
  end

  state do |id|
    []
  end

  command :send_email, cart_id: String do |_, cmd|
    # Send email here, emit EmailSent if successful
    event(EmailSent, cart_id: cmd.payload.cart_id)
  end

  event EmailSent do |list, event|
    list << event
  end
end

# A Saga that orchestrates the flow between Cart and Mailer
class CartEmailsSaga < Sourced::Actor
  # Listen for Cart::Placed events and
  # send command to Mailer
  reaction Cart::Placed do |event|
    dispatch(Mailer::SendEmail, cart_id: event.stream_id).to("mailer-#{event.stream_id}")
  end

  # Listen for Mailer::EmailSent events and
  # send command to Cart
  reaction Mailer::EmailSent do |event|
    dispatch(Cart::Notify, mailer_id: event.stream_id).to(event.payload.cart_id)
  end
end

# A projector
# "reacts" to events registered with .evolve
class CartListings < Sourced::Actor
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

  event Cart::Placed do |cart, event|
    cart[:status] = :placed
  end

  event Cart::ItemAdded do |cart, event|
    cart[:items] << event.payload.to_h
  end
end

class LoggingReactor
  extend Sourced::Consumer

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
    # @option replaying [Boolean] whether this is a replay of events
    # @return [Array<Message]
    def handle_events(events, replaying:)
      puts "LoggingReactor received #{events}"
      []
    end
  end
end

# Cart.sync CartListings

# Register Reactor interfaces with the Router
# This allows the Router to route commands and events to reactors
Sourced.register(LoggingReactor)
Sourced.register(Cart)
Sourced.register(Mailer)
Sourced.register(CartEmailsSaga)
Sourced.register(CartListings)
