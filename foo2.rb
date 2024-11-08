require 'sors/aggregate'

class TodoList < Sors::Aggregate
  Item = Struct.new(:id, :name, :done)

  attr_reader :email_sent

  def initialize(id)
    super(id)
    @email_sent = false
    @items = {}
  end

  def items = @items.values

  ##########################################################
  ### Option 1: Explicit command and method definition #####
  ##########################################################
  AddItem = Sors::Message.define('todos.items.add') do
    attribute :name, String
  end

  # Handle commands and decide what events
  # to apply
  decide AddItem do |cmd|
    item_id = SecureRandom.uuid
    cmd.follow(ItemAdded, item_id:, name: cmd.payload.name)
  end

  # A helper method to build and apply a command
  # Exaple:
  #   list = TodoList.create
  #   list.add_item(name: 'Buy milk')
  def add_item(name:)
    command AddItem, name:
  end

  ##########################################################
  ### Option 2: .command DSL to define command and method ##
  ##########################################################
  # Define the three steps above in one go.
  # This defines a local constant AddItem command
  # Registers the .decide command handler
  # and defines an #add_item method on the instance
  # Example:
  #   list.add_item(name: 'Buy milk')
  command :add_item, name: String do |cmd|
    item_id = SecureRandom.uuid
    cmd.follow(ItemAdded, item_id:, name: cmd.payload.name)
  end

  ## Durable execution
  step :book_hotel, hotel_id: Integer, check_in: Date, check_out: Date do |args|
    result = HotelsAPI.book(args[:hotel_id], args[:check_in], args[:check_out])
  end



  decide MarkDone do |cmd|
    raise 'no item with that id' unless @items.key?(cmd.payload.item_id)
    cmd.follow(ItemDone, item_id: cmd.payload.item_id)
  end

  # Handle events and evolve the interbal state
  evolve ItemAdded do |event|
    item_id = event.payload.item_id
    @items[item_id] = Item.new(item_id, event.payload.name, false)
  end

  evolve ItemDone do |event|
    @items[event.payload.item_id].done = true
  end

  # React to events and decide what 
  # command to issue next.
  # This will be handled in a background process
  react ItemDone do |event|
    if all_items_done?
      event.follow(SendEmail)
    end
  end

  decide SendEmail do |cmd|
    if Mailer.deliver_list_completed(self)
      cmd.follow(EmailSent)
    else
      cmd.follow(EmailFailed)
    end
  end

  evolve EmailSent do |event|
    @email_sent = true
  end

  def mark_done(item_id)
    command MarkDone, item_id:
  end

  def all_items_done?
    items.size == items.filter(&:done).size
  end
end
