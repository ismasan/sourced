# frozen_string_literal: true

# LiteCart — A shopping cart demo using Sourced with the SQLite backend.
#
# This example demonstrates:
#   - Defining an Actor with commands, events, and reactions
#   - Using the SQLite backend (no Postgres required)
#   - Using CommandMethods to call commands as regular methods
#   - Persisting events via bang methods (add_item!, place_order!, etc.)
#   - Reading back the event stream
#   - Reloading state from persisted events
#
# Run it:
#   bundle exec ruby examples/lite_cart.rb
#
# What happens:
#   1. Creates an in-memory SQLite database
#   2. Adds items to a cart, removes one, then places the order
#   3. Prints the full event stream and the rebuilt cart state

require 'bundler/setup'
require 'sourced'
require 'sourced/command_methods'
require 'sequel'

# --- Configure the SQLite backend -------------------------------------------
# Passing a Sequel SQLite connection auto-selects SQLiteBackend.
# Use Sequel.sqlite('carts.db') for a file-based database.
Sourced.configure do |config|
  config.backend = Sequel.sqlite
end

Sourced.config.backend.install unless Sourced.config.backend.installed?

# --- Define the LiteCart Actor -----------------------------------------------
# An Actor encapsulates state, command handling (decide), event handling (evolve),
# and reactions (side-effects triggered by events).
#
# Including CommandMethods generates convenience instance methods
# for each command:
#   cart.add_item(name: 'X', price: 100)   # in-memory only
#   cart.add_item!(name: 'X', price: 100)  # persists to backend
class LiteCart < Sourced::Actor
  include Sourced::CommandMethods

  # Cart state — a plain Struct rebuilt from events on every load.
  State = Struct.new(:status, :items, :total, keyword_init: true)

  state do |_id|
    State.new(status: :open, items: [], total: 0)
  end

  # --- Commands (intent to change) ------------------------------------------
  # Each `command` block defines a command message class (e.g. LiteCart::AddItem)
  # and a handler that validates + produces events.

  command :add_item, name: String, price: Integer do |cart, cmd|
    raise 'Cart is not open' unless cart.status == :open

    event :item_added, cmd.payload.to_h
  end

  command :remove_item, name: String do |cart, cmd|
    raise 'Cart is not open' unless cart.status == :open

    item = cart.items.find { |i| i[:name] == cmd.payload.name }
    raise "Item #{cmd.payload.name} not in cart" unless item

    event :item_removed, name: item[:name], price: item[:price]
  end

  command :place_order do |cart, _cmd|
    raise 'Cart is not open'  unless cart.status == :open
    raise 'Cart is empty'     if cart.items.empty?

    event :order_placed, item_count: cart.items.size, total: cart.total
  end

  # --- Events (facts that happened) -----------------------------------------
  # Each `event` block evolves the state. Events are persisted and replayed
  # to rebuild state. They must be pure — no side effects.

  event :item_added, name: String, price: Integer do |cart, evt|
    cart.items << { name: evt.payload.name, price: evt.payload.price }
    cart.total += evt.payload.price
  end

  event :item_removed, name: String, price: Integer do |cart, evt|
    cart.items.reject! { |i| i[:name] == evt.payload.name }
    cart.total -= evt.payload.price
  end

  event :order_placed, item_count: Integer, total: Integer do |cart, _evt|
    cart.status = :placed
  end

  # --- Reactions (side-effects) ----------------------------------------------
  # Reactions run asynchronously (via workers) after events are persisted.
  # They can dispatch new commands to other actors.
  # Note: reactions do NOT fire when using CommandMethods directly.
  # Use Sourced::Unit or background workers to trigger them.

  reaction :order_placed do |state, event|
    puts "  -> [reaction] Order placed! #{state.items.size} items, total: #{state.total}"
  end
end

# =============================================================================
# Demo script
# =============================================================================
puts "=== LiteCart Demo (SQLite backend) ===\n\n"

# Create a cart instance with a specific stream ID.
cart, evts = Sourced.load(LiteCart, 'cart-1')
# cart = LiteCart.new(id: 'cart-1')

# Use bang methods (add_item!) to persist events to the SQLite backend.
puts "--- Adding items ---"
_cmd, events = cart.add_item!(name: 'Notebook', price: 1200)
puts "  Added Notebook (1200) -> #{events.map(&:type)}"

_cmd, events = cart.add_item!(name: 'Pen', price: 300)
puts "  Added Pen (300) -> #{events.map(&:type)}"

_cmd, events = cart.add_item!(name: 'Eraser', price: 50)
puts "  Added Eraser (50) -> #{events.map(&:type)}"

puts "\n--- Removing an item ---"
_cmd, events = cart.remove_item!(name: 'Eraser')
puts "  Removed Eraser -> #{events.map(&:type)}"

puts "\n--- Placing order ---"
_cmd, events = cart.place_order!
puts "  Order placed -> #{events.map(&:type)}"

puts "\n--- Current cart state (in-memory) ---"
puts "  status: #{cart.state.status}"
puts "  items:  #{cart.state.items}"
puts "  total:  #{cart.state.total}"

# Read the full event stream back from the database.
puts "\n--- Event stream for cart-1 ---"
events = Sourced.config.backend.read_stream('cart-1')
events.each do |e|
  puts "  seq:#{e.seq}  #{e.type.ljust(30)}  #{e.payload.to_h}"
end

# Reload a fresh cart from persisted events to show state is rebuilt.
puts "\n--- Reload cart from events ---"
reloaded, _events = Sourced.load(LiteCart, 'cart-1')
puts "  status: #{reloaded.state.status}"
puts "  items:  #{reloaded.state.items}"
puts "  total:  #{reloaded.state.total}"

puts "\nDone!"
