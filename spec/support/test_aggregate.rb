# frozen_string_literal: true

module TestAggregate
  class TodoList < Sors::Aggregate
    MarkDone = Sors::Message.define('todos.items.mark_done') do
      attribute :item_id, String
    end
    SendEmail = Sors::Message.define('todos.emails.send')

    ItemAdded = Sors::Message.define('todos.items.added') do
      attribute :item_id, String
      attribute :name, String
    end
    ItemDone = Sors::Message.define('todos.items.done') do
      attribute :item_id, String
    end
    EmailSent = Sors::Message.define('todos.emails.sent')

    Item = Struct.new(:id, :name, :done)

    attr_reader :email_sent

    def setup(id)
      @email_sent = false
      @items = {}
    end

    def items = @items.values

    command :add, 'todos.items.add', name: String do |cmd|
      item_id = SecureRandom.uuid
      cmd.follow(ItemAdded, item_id:, name: cmd.payload.name)
    end

    command :complete_by_name, 'todos.items.complete_by_name', name: String do |cmd|
      item = items.find { |it| it.name == cmd.payload.name  }
      if item && !item.done
        cmd.follow(ItemDone, item_id: item.id)
      end
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

    def mark_done(item_id)
      issue_command MarkDone, item_id:
    end

    def all_items_done?
      items.size == items.filter(&:done).size
    end
  end

  Sors::Router.register(TestAggregate::TodoList)
end

