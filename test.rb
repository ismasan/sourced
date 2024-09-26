require 'bundler/setup'
require 'sequel'
require 'json'
require 'digest/md5'

module Sequel
  def self.parse_json(json)
    JSON.parse(json, symbolize_names: true)
  end
end

DB = Sequel.postgres('decider')
DB.extension :pg_json

class Commander
  attr_reader :name

  def initialize(db, name = nil)
    @db = db
    @running = false
    @name = name
  end

  # Machine::Scheduler interface
  def call(commands)
    return false if commands.empty?

    # TODO: here we could use multi_insert
    # for both streams and commands
    db.transaction do
      commands.each do |command|
        add(command.stream_id, command)
      end
    end
  end

  def add(stream_id, command)
    db.transaction do
      db[:streams].insert_conflict.insert(stream_id:)
      db[:commands].insert(stream_id:, data: command.to_json)
    end
  end

  def stop
    @running = false
  end

  def poll(&)
    @running = true
    while @running
      # sleep 0.5 unless reserve_next(&)
      reserve_next(&)
      # This sleep seems to be necessary or workers in differet processes will not be able to get the lock
      sleep 0.2
    end
    puts "Worker #{name}: Polling stopped"
  end

  def reserve_next(&)
    command = db.transaction do
      cmd = db[:commands]
        .join(:streams, stream_id: :stream_id)
        .where(Sequel[:streams][:locked] => false)
        .order(Sequel[:commands][:id])
        .for_update
        .first

      if cmd
        db[:streams].where(stream_id: cmd[:stream_id]).update(locked: true)
      end
      cmd
    end

    if command
      yield Message.from(command[:data])
      # Only delete the command if processing didn't raise
      db[:commands].where(id: command[:id]).delete
    end
    command
  ensure
    # Always unlock the stream
    if command
      db[:streams].where(stream_id: command[:stream_id]).update(locked: false)
    end
  end

  # def reserve(stream_id, &)
  #   command = db.transaction do
  #     cmd = db[:commands]
  #       .join(:streams, stream_id: :stream_id)
  #       .where(stream_id:)
  #       .order(Sequel[:commands][:id]).first
  #     if lock(stream_id)
  #       db[:commands].where(stream_id:).order(Sequel[:commands][:id]).first
  #     else
  #       nil
  #     end
  #   end
  #
  #   return nil unless command
  #
  #   yield command if block_given?
  #   db[:commands].where(id: command[:id]).delete
  #   command
  # ensure
  #   unlock(stream_id)
  # end

  def lock(stream_id)
    db.fetch('select pg_try_advisory_lock(?) AS lock', hash(stream_id)).first[:lock]
  end

  def unlock(stream_id)
    db.fetch('select pg_advisory_unlock(?) AS lock', hash(stream_id)).first[:lock]
  end

  def hash(str)
    # binary_string = ('0' + Digest::MD5.hexdigest(str)[0...16]).hex.to_s(2)
    binary_string = ('0' + Digest::MD5.hexdigest(str)[0...10]).hex
    return binary_string
    # Convert the binary string to a signed BigInt
    bigint_value = binary_string.to_i(2)

    # Check if the most significant bit (bit 63) is set (indicating a negative value in 2's complement)
    if binary_string[0] == '1'
      # Handle negative value by applying 2's complement
      bigint_value -= 2**64
    end

    bigint_value.abs
  end

  private

  attr_reader :db
end

COMMANDS = Commander.new(DB)

# commands.add('cart-123', { name: 'add_item', payload: { sku: '111' } })
# commands.add('cart-123', { name: 'add_item', payload: { sku: '222' } })
# p DB[:commands].first[:data]
require_relative 'machine'
require_relative 'message'
require_relative 'event_store'

class CartMachine < Machine
  self.scheduler = COMMANDS

  Cart = Struct.new(:id, :items, :total)

  AddItem = Message.define('carts.items.add')
  SendEmail = Message.define('carts.send_email')
  ItemAdded = Message.define('carts.items.added')
  EmailSent = Message.define('carts.email_sent')
  PlaceOrder = Message.define('carts.place')
  OrderPlaced = Message.define('carts.placed')

  load do |command|
    Cart.new(command.stream_id, [], 0)
  end

  decide AddItem do |cart, command|
    command.follow(ItemAdded)
  end

  evolve ItemAdded do |cart, event|
    cart
  end

  react ItemAdded do |event|
    event.follow(SendEmail)
  end

  decide SendEmail do |cart, command|
    [command.follow(EmailSent)]
  end

  evolve EmailSent do |cart, event|
    cart
  end

  react EmailSent do |event|
    event.follow(PlaceOrder)
  end

  decide PlaceOrder do |cart, command|
    command.follow(OrderPlaced)
  end

  attr_reader :event_store

  def initialize(event_store)
    super()
    @event_store = event_store
  end

  def persist(cart, command, events)
    puts "Persisting #{cart}, #{command}, #{events}"
    event_store.append([command, *events])
  end
end

class SalesReportSaga < Reactor
  react CartMachine::ItemAdded do |event|
    puts "Saga: Item added #{event.payload}"
    []
  end
end

# TODO: perhaps we can just Router.register(Thing)
# Thing will be registered as Machine, Reactor, Projection, etc
# (or many of them)
# depending on the interfaces it implements
# This way its easy to compose workflows that may involve handling commands, events or reactions.
# Some may load an initial entity, some others may not.
ES = EventStore.new(DB)
Router.register_machine(CartMachine.new(ES))
Router.register_reactor(SalesReportSaga)
