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

      def initialize(id)
        @id = id
        @items = {}
      end

      def total
        items.values.sum do |item|
          item.quantity * item.price
        end
      end

      def add_item(product_id:, quantity:, product_name:, price:)
        items[product_id] = Item.parse(product_id:, quantity:, product_name:, price:)
      end
    end

    AddItem = Sors::Message.define('carts.items.add') do
      attribute :product_id, Integer
      attribute :quantity, Sors::Types::Integer.default(1)
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

    EmailSent = Sors::Message.define('carts.email_sent')
    PlaceOrder = Sors::Message.define('carts.place')
    OrderPlaced = Sors::Message.define('carts.placed')

    load do |command|
      EntityStore.instance.load(command.stream_id) || Cart.new(command.stream_id)
    end

    decide AddItem do |_cart, cmd|
      product = CATALOG[cmd.payload.product_id]
      if product
        cmd.follow(ItemAdded, cmd.payload.to_h.merge(product))
      else
        cmd.follow(NoItemAdded, product_id: cmd.payload.product_id)
      end
    end

    evolve ItemAdded do |cart, event|
      cart.add_item(**event.payload.to_h)
    end

    react ItemAdded do |event|
      event.follow(SendEmail)
    end

    decide SendEmail do |_cart, command|
      [command.follow(EmailSent)]
    end

    evolve EmailSent do |cart, _event|
      cart
    end

    react EmailSent do |event|
      event.follow(PlaceOrder)
    end

    decide PlaceOrder do |_cart, command|
      command.follow(OrderPlaced)
    end

    def persist(cart, command, events)
      Sors.config.logger.info "Persisting #{cart}, #{command}, #{events}"
      backend.append_events([command, *events])
      EntityStore.instance.save(cart)
    end
  end

  Sors::Router.register(Carts.new)
end
