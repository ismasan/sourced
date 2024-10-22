# frozen_string_literal: true

module TestDomain
  class EntityStore
    include Singleton

    def initialize
      @entities = {}
    end

    def save(entity)
      @entities[entity.id] = entity
    end

    def load(stream_id)
      @entities[stream_id]
    end
  end

  class WebhookReceiver
    include Singleton

    attr_reader :webhooks

    def initialize
      @webhooks = []
    end

    def post(webhook)
      @webhooks << webhook
    end
  end

  class ItemCounter
    include Singleton
    def self.inc
      instance.inc
    end

    attr_reader :count
    def initialize
      @count = 0
    end

    def inc
      @count += 1
    end
  end

  CATALOG = {
    1 => { product_name: 'Apple', price: 100 },
    2 => { product_name: 'Banana', price: 50 },
  }.freeze

  class Carts < Sors::Machine
    class Cart
      Item = Sors::Types::Data[product_id: Integer, quantity: Integer, product_name: String, price: Integer]

      attr_reader :items, :id
      attr_accessor :webhooks_sent, :status, :event_count

      def initialize(id)
        @id = id
        @items = {}
        @webhooks_sent = 0
        @status = :open
        @event_count = 0
      end

      def total
        items.values.sum do |item|
          item.quantity * item.price
        end
      end

      def add_item(product_id:, quantity:, product_name:, price:)
        items[product_id] = Item.parse(product_id:, quantity:, product_name:, price:)
      end

      def remove_item(product_id)
        items.delete(product_id)
      end
    end

    AddItem = Sors::Message.define('carts.items.add') do
      attribute :product_id, Integer
      attribute :quantity, Sors::Types::Integer.default(1)
    end
    RemoveItem = Sors::Message.define('carts.items.remove') do
      attribute :product_id, Integer
    end

    SendItemAddedWebhook = Sors::Message.define('carts.send_item_added_webhook') do
      attribute :cart_total, Integer
      attribute :product_id, Integer
    end

    ItemAdded = Sors::Message.define('carts.items.added') do
      attribute :product_id, Integer
      attribute :quantity, Sors::Types::Integer.default(1)
      attribute :product_name, String
      attribute :price, Integer
    end
    NoItemAdded = Sors::Message.define('carts.items.not_added') do
      attribute :product_id, Integer
    end
    ItemRemoved = Sors::Message.define('carts.items.removed') do
      attribute :product_id, Integer
    end

    ItemAddedWebhookSent = Sors::Message.define('carts.item_added_webhook_sent')
    PlaceOrder = Sors::Message.define('carts.place')
    OrderPlaced = Sors::Message.define('carts.placed')

    # Just here to test inheritance
    RandomCommand = Sors::Message.define('carts.random_command')
    RandomEvent = Sors::Message.define('carts.random_event')

    decide AddItem do |_cart, cmd|
      product = CATALOG[cmd.payload.product_id]
      if product
        cmd.follow(ItemAdded, cmd.payload.to_h.merge(product))
      else
        cmd.follow(NoItemAdded, product_id: cmd.payload.product_id)
      end
    end

    decide RemoveItem do |cart, cmd|
      if cart.items[cmd.payload.product_id]
        cmd.follow(ItemRemoved, cmd.payload.to_h)
      else
        raise 'Item not found'
      end
    end

    evolve :any do |cart, event|
      cart.event_count += 1
    end

    evolve ItemAdded do |cart, event|
      cart.add_item(**event.payload.to_h)
    end

    evolve ItemRemoved do |cart, event|
      cart.remove_item(event.payload.product_id)
    end

    react ItemAdded do |cart, event|
      event.follow(SendItemAddedWebhook, cart_total: cart.total, product_id: event.payload.product_id)
    end

    react_sync ItemAdded do |_cart, event|
      ItemCounter.inc
      nil
    end

    decide SendItemAddedWebhook do |_cart, command|
      WebhookReceiver.instance.post(command)
      [command.follow(ItemAddedWebhookSent)]
    end

    evolve ItemAddedWebhookSent do |cart, _event|
      cart.webhooks_sent += 1
    end

    # ==== State-stored version ==================
    # load cart from DB, or instantiate a new one
    # def load(command)
    #   Cart.find(command.stream_id) || Cart.new(command.stream_id)
    # end

    # Save updated cart to DB, optionally save new events
    # for full audit trail
    # def save(cart, command, events)
    #   backend.append_events([command, *events])
    #   cart.save!
    # end

    # ==== Event-sourced version ==================
    # Initialize a new cart and apply all previous events
    # to get current state.
    def load(command)
      cart = Cart.new(command.stream_id)
      events = backend.read_event_stream(command.stream_id)
      evolve(cart, events)
    end

    # Save new events to the event store
    def save(cart, command, events)
      Sors.config.logger.info "Persisting #{cart}, #{command}, #{events} to #{backend.inspect}"
      backend.append_events([command, *events])
      EntityStore.instance.save(cart)
    end

    def self.replay(stream_id) = new.replay(stream_id)

    def replay(stream_id)
      cart = Cart.new(stream_id)
      events = backend.read_event_stream(stream_id)
      evolve(cart, events)
    end
  end

  Sors::Router.register(Carts)
end

