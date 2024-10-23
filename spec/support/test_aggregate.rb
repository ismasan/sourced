# frozen_string_literal: true

require 'sors/aggregate'

module TestAggregate
  class TodoList < Sors::Aggregate
    AddItem = Sors::Message.define('todos.items.add') do
      attribute :name, String
    end
    MarkDone = Sors::Message.define('todos.items.mark_done') do
      attribute :item_id, Integer
    end
    SendEmail = Sors::Message.define('todos.emails.send')

    ItemAdded = Sors::Message.define('todos.items.added') do
      attribute :item_id, Integer
      attribute :name, String
    end
    ItemDone = Sors::Message.define('todos.items.done') do
      attribute :item_id, Integer
    end
    EmailSent = Sors::Message.define('todos.emails.sent')

    Item = Struct.new(:id, :name, :done)

    attr_reader :email_sent

    def initialize(id)
      super(id)
      @email_sent = false
      @items = {}
    end

    def items = @items.values

    # Define a command class
    # and a method to invoke it
    # Example:
    #   list.add(name: 'Buy milk')
    command :add do
      payload do
        attribute :name, String
      end
      run do |cmd|
        item_id = SecureRandom.uuid
        cmd.follow(ItemAdded, item_id:, name: cmd.payload.name)
      end
    end

    decide AddItem do |cmd|
      item_id = SecureRandom.uuid
      cmd.follow(ItemAdded, item_id:, name: cmd.payload.name)
    end

    decide MarkDone do |cmd|
      raise 'no item with that id' unless @items.key?(cmd.payload.item_id)
      cmd.follow(ItemDone, item_id: cmd.payload.item_id)
    end

    evolve ItemAdded do |event|
      item_id = event.payload.item_id
      @items[item_id] = Item.new(item_id, event.payload.name, false)
    end

    evolve ItemDone do |event|
      @items[event.payload.item_id].done = true
    end

    react ItemDone do |event|
      if all_items_done?
        event.follow(SendEmail)
      end
    end

    decide SendEmail do |cmd|
      cmd.follow(EmailSent)
    end

    evolve EmailSent do |event|
      @email_sent = true
    end

    # def add(name)
    #   command AddItem, name:
    # end

    def mark_done(item_id)
      command MarkDone, item_id:
    end

    def all_items_done?
      items.size == items.filter(&:done).size
    end
  end

  Sors::Router.register(TestAggregate::TodoList)
end

