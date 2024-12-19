# sourced

**WORK IN PROGRESS**

Event Sourcing / CQRS library for Ruby.
There's many ES gems available already. The objectives here are:
* Cohesive and toy-like DX.
* Eventual consistency by default. Actor-like execution model.
* Built around the [Decide, Evolve, React pattern](https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/)
* Control concurrency by modeling.
* Simple to operate: it should be as simple to run as most Ruby queuing systems.
* Explore ES as a programming model for Ruby apps.



### The programming model

If you're unfamiliar with Event Sourcing, you can read this first: [Event Sourcing from the ground up, with Ruby examples](https://ismaelcelis.com/posts/event-sourcing-ruby-examples)

The entire behaviour of an event-sourced app is described via **commands**, **events** and **reactions**.

![command, events, reactions](docs/images/sourced-arch-diagram.png)

* **Commands** are _intents_ to effect some change in the state of the system. Ex. `Add cart item`, `Place order`, `Update email`, etc.
* **Events** are produced after handling a command and they describe _facts_ or state changes in the system. Ex. `Item added to cart`, `order placed`, `email updated`. Events are stored and you can use them to build views ("projections"), caches and reports to support UIs, or other artifacts.
* **State** is whatever object you need to hold the current state of a part of the system. It's usually derived from past events, and it's just enough to interrogate the state of the system and make the next decision.

### Deciders

Deciders are classes that encapsulate loading state from past events and handling commands for a part of your system. They can also define reactions to their own events, or events emitted by other deciders. This is a simple shopping cart decider.

```ruby
class Cart < Sourced::Decider
  # Define what cart state looks like.
  # This is the initial state which will be updated by applying events.
  # The state holds whatever data is relevant to decide how to handle a command.
  # It can be any object you need. A custom class instance, a Hash, an Array, etc.
  CartState = Struct.new(:id, :status, :items) do
    def total = items.sum { |it| it.price * it.quantity }
  end
    
  CartItem = Struct.new(:product_id, :price, :quantity)
    
  # This factory is called to initialise a blank cart.
  state do |id|
    CartState.new(id:, status: 'open', items: [])
  end
  
  # Define a command and its handling logic.
  # The command handler will be passed the current state of the cart,
  # and the command instance itself.
  # Its main job is to validate business rules and decide whether new events
  # can be emitted to update the state
  command :add_item, product_id: String, price: Integer, quantity: Integer do |cart, cmd|
    # Validate that this command can run
    raise "cart is not open!" unless cart.status == 'open'
    # Produce a new event with the same attributes as the command
    event :item_added, cmd.payload
  end
  
  # Define an event handler that will "evolve" the state of the cart by adding an item to it.
  # These handlers are also used to "hydrate" the initial state from Sourced's storage backend
  # when first handling a command
  event :item_added, product_id: String, price: Integer, quantity: Integer do |cart, event|
    cart.items << CartItem.new(**event.payload.to_h)
  end
  
  # Optionally, define how this decider reacts to the event above.
  # .react blocks can dispatch new commands that will be routed to their handlers.
  # This allows you to build workflows.
  # TODO: reacting to own events should provide state?
  react :item_added do |event|
    # Evaluate whether we should dispatch the next command.
    # Here we could fetch some external data or query that might be needed
    # to populate the new commands.
    command :send_admin_email, product_id: event.payload.product_id
  end
  
  # Handle the :send_admin_email dispatched by the reaction above
  command :send_admin_email, product_id: String do |cart, cmd|
    # maybe produce new events
  end
end
```

Using the `CartDecider` in an IRB console. This will use Sourced's in-memory backend by default.

```ruby
cart = Cart.new('test-cart')
cart.state.total # => 0
cmd = cart.add_item(product_id: 'p123', price: 1000, quantity: 2)
cmd.valid? # true
# Inspect state
cart.state.total # 2000
cart.items.items.size # 1
# Inspect that events were stored
cart.seq # 2 the sequence number or "version" in storage. Ie. how many commands / events exist for this cart
cart.events # an array with instances of [Cart::AddItem, Cart::ItemAdded]
cart.events.map(&:type) # ['cart.add_item', 'cart.item_added']
```

Try loading a new cart instance from recorded events

```ruby
cart2 = Cart.load('test-cart')
cart2.seq # 2
cart2.stats.total # 2000
cart2.state.items.size # 1
```

#### Registering deciders

Invoking commands directly on a decider instance works in an IRB console or a synchronous-only web handler, but for deciders to be available for background workers, and to react to other decider's events, you need to register them.

```ruby
Sourced::Router.register(Cart)
```

This achieves two things:

1. Commands can be routed to this decider by background processes, using its `.handle_command(command)` interface
2. The decider can _react_ to other events in the system (more on event choreography later), via its `.handle_events(events)` interface.

This two properties are what enables asynchronous, eventually-consistent systems in Sourced.

#### Expanded message syntax

Commands and event structs can also be defined separately as `Sourced::Command` and `Sourced::Event` sub-classes.

These definitions include a message _type_ (for storage) and payload attributes schema, if any.

```ruby
module Carts
  # A command to add an item to the cart
  # Commands may come from HTML forms, so we use Types::Lax to coerce attributes
  AddItem = Sourced::Command.define('carts.add_item') do
    attribute :product_id, Types::Lax::Integer
    attribute :quantity, Types::Lax::Integer.default(1)
    attribute :price, Types::Lax::Integer.default(0)
  end
  
  # An event to track items added to the cart
  # Events are only produced by valid commands, so we don't 
  # need validations or coercions
  ItemAdded = Sourced::Event.define('carts.item_added') do
    attribute :product_id, Integer
    attribute :quantity, Integer
    attribute :price, Integer
  end
  
  ## Now define command and event handlers in a Decider
  class Cart < Sourced::Decider
    # Initial state, etc...
    
    command AddItem do |cart, cmd|
      # logic here
      event ItemAdded, cmd.payload
    end
    
    event ItemAdded do |cart, event|
      cart.items << CartItem.new(**event.payload.to_h)
    end
  end
end
```

### Causation and correlation

When a command produces events, or when an event makes a reactor dispatch a new command, the cause-and-effect relationship between these messages is tracked by Sourced in the form of `correlation_id` and `causation_id` properties in each message's metadata.

![causation and correlation](docs/images/sourced-causation-correlation.png)

This helps the system keep a full audit trail of the cause-and-effect behaviour of the entire system.

![command and event causation view](docs/images/sourced-causation-view.png)

### Background vs. foreground execution

TODO

### Projectors

From the outside-in, projectors are classes that implement the _Reactor interface_.

Sourced ships with two ready-to-use projectors, but you can also build your own.

#### State-stored projector

A state-stored projector fetches initial state from storage somewhere (DB, files, API), and then after reacting to events and updating state, it can save it back to the same or different storage.

```ruby
class CartListings < Sourced::Projector::StateStored
  # Fetch listing record from DB, or new one.
  state do |id|
    CartListing.find_or_initialize(id)
  end

  # Evolve listing record from events
  event Carts::ItemAdded do |listing, event|
    listing.total += event.payload.price
  end

  # Sync listing record back to DB
  sync do |listing, _, _|
    listing.save!
  end
end
```

#### Event-sourced projector

An event-sourced projector fetches initial state from past events in the event store, and then after reacting to events and updating state, it can save it to a DB table, a file, etc.

```ruby
class CartListings < Sourced::Projector::EventSourced
  # Initial in-memory state
  state do |id|
    { id:, total: 0 }
  end

  # Evolve listing record from events
  event Carts::ItemAdded do |listing, event|
    listing[:total] += event.payload.price
  end

  # Sync listing record to a file
  sync do |listing, _, _|
    File.write("/listings/#{listing[:id]}.json", JSON.dump(listing)) 
  end
end
```

#### Registering projectors

Like any other _reactor_, projectors need to be registered for background workers to route events to them.

```ruby
# In your app's configuration
Sourced::Router.register(CartListings)
```

## Concurrency model

Concurrency in Sourced is achieved by explicitely _modeling it in_.

Sourced workers process events and commands by acquiring locks on `[reactor group ID][stream ID]`.

This means that all events for a given reactor/stream are processed in order, but events for different streams can be processed concurrently. You can define workflows where some work is done concurrently by modeling them as a collaboration of streams.

#### Single-stream sequential execution

In the following (simplified!) example, a Holiday Booking workflow is modelled as a single stream ("Decider"). The infrastructure makes sure these steps are run sequentially.

![Concurrency single stream](docs/images/sourced-concurrency-single-lane.png)

The Decider glues its steps together by reacting to events emitted by the previous step, and dispatching the next command.

```ruby
class HolidayBooking < Sourced::Decider
  # State and details omitted...
  
  command :start_booking do |state, cmd|
    event :booking_started
  end
  
  react :booking_started do |event|
    command :book_flight
  end
  
  command :book_flight do |state, cmd|
    event :flght_booked
  end
  
  react :flight_booked do |event|
    command :book_hotel
  end
  
  command :book_hotel do |state, cmd|
    event :hotel_booked
  end
  
  # Define event handlers if you haven't...
  event :booking_started, # ..etc
  event :flight_booked, # ..etc
end
```

#### Multi-stream concurrent execution

In this other example, the same workflow is split into separate streams/deciders, so that Flight and Hotel bookings can run concurrently from each other. When completed, they each notify the parent Holiday decider, so the whole process coalesces into a sequential operation again.

![multi stream](docs/images/sourced-concurrency-multi-lane.png)

TODO: code example.

### Orchestration and choreography

TODO

### Transactional boundaries

![transactional boundaries](docs/images/sourced-transactional-boundaries.png)

The diagram shows the units of work in an example Sourced worklow. The operations within each of the red boxes either succeeds or rolls back the transaction, and it can then be retried or compensated. They are **strongly consistent**. 
The data-flow _between_ these boxes is propagated asynchronously by Sourced's infrastructure so, relative to each other, the entire system is **eventually consistent**.

These transactional boundaries are also guarded by the same locks that enforce the [concurrency model](#concurrency-model), so that for example the same event or command can't be processed by the same Decider or Reactor (workflow, projector, etc). 

### Scheduled commands

TODO

### Replaying events

TODO

## Interfaces

TODO

### Testing

TODO

## Setup

You'll need the `pg` and `sequel` gems.

```ruby
gem 'sourced', github: 'ismasan/sourced'
gem 'pg'
gem 'sequel'
```

Create a Postgres database.
For now Sourced uses the Sequel gem. In future there'll be an ActiveRecord adapter with migrations support.

Configure and migrate the database.

```ruby
Sourced.configure do |config|
  config.backend = Sequel.connect(ENV.fetch('DATABASE_URL'))
end

Sourced.config.backend.install unless Sourced.config.backend.installed?
```

Register your Deciders and Reactors.

```ruby
Sourced::Router.register(Leads::Decider)
Sourced::Router.register(Leads::Listings)
Sourced::Router.register(Webooks::Dispatcher)
```

Start background workers.

```ruby
# require your code here
Sourced::Supervisor.start(count: 10) # 10 worker fibers
```

### Custom attribute types and coercions.

Define a module to hold your attribute types using [Plumb](https://github.com/ismasan/plumb)

```ruby
module Types
  include Plumb::Types
  
  # Your own types here.
  CorporateEmail = Email[/^apple@/]
end
```

Then you can use any [built-in Plumb types](https://github.com/ismasan/plumb?tab=readme-ov-file#built-in-types), as well as your own, when defining command or event structs (or any other data structures for your app).

```ruby
UpdateEmail = Sourced::Command.define('accounts.update_email') do
  attribute :email, Types::CorporateEmail
end
```



## Rails integration

Soon.

## Sourced vs. ActiveJob

ActiveJob is a great way to handle background jobs in Rails. It's simple and easy to use. However, it's not designed for event sourcing.
ActiveJob backends (and other job queues) are optimised for parallel processing of jobs, this means that multiple jobs for the same business entity may be processed in parallel without any ordering guarantees.

![job queue concurrency](docs/images/sourced-job-queue-diagram.png)

Sourced's concurrency model is designed to process events for the same entity in order, while allowing for parallel processing of events for different entities.

![job queue concurrency](docs/images/sourced-ordered-streams-diagram.png)

## Installation

Install the gem and add to the application's Gemfile by executing:

    $ bundle add sourced

**Note**: this gem is under active development, so you probably want to install from Github:
In your Gemfile:

    $ gem 'sourced', github: 'ismasan/sourced'

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/sourced.
