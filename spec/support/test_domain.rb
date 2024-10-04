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

  CATALOG = {
    1 => { product_name: 'Apple', price: 100 },
    2 => { product_name: 'Banana', price: 50 },
  }.freeze

  class Carts < Sors::Machine
    class Cart
      Item = Sors::Types::Data[product_id: Integer, quantity: Integer, product_name: String, price: Integer]

      attr_reader :items, :id
      attr_accessor :email_sent, :status

      def initialize(id)
        @id = id
        @items = {}
        @email_sent = false
        @status = :open
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

    SendEmail = Sors::Message.define('carts.send_email')

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

    EmailSent = Sors::Message.define('carts.email_sent')
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

    evolve ItemAdded do |cart, event|
      cart.add_item(**event.payload.to_h)
    end

    evolve ItemRemoved do |cart, event|
      cart.remove_item(event.payload.product_id)
    end

    react ItemAdded do |event|
      event.follow(SendEmail)
    end

    decide SendEmail do |_cart, command|
      [command.follow(EmailSent)]
    end

    evolve EmailSent do |cart, _event|
      cart.email_sent = true
    end

    react EmailSent do |event|
      event.follow(PlaceOrder)
    end

    decide PlaceOrder do |_cart, command|
      command.follow(OrderPlaced)
    end

    evolve OrderPlaced do |cart, _event|
      cart.status = :placed
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

    def replay(stream_id)
      cart = Cart.new(stream_id)
      events = backend.read_event_stream(stream_id)
      cart = evolve(cart, events)
      cart
    end
  end

  Sors::Router.register(Carts.new)
end

