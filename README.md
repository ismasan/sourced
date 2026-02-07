# sourced

**WORK IN PROGRESS**

Event Sourcing / CQRS library for Ruby.
There's many ES gems available already. The objectives here are:
* Cohesive and toy-like DX.
* Eventual consistency by default. Actor-like execution model.
* Low-level APIs for durable messaging.
* Supports the [Decide, Evolve, React pattern](https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/)
* Control concurrency by modeling.
* Simple to operate: it should be as simple to run as most Ruby queuing systems.
* Explore ES as a programming model for Ruby apps.

A small demo app [here](https://github.com/ismasan/sourced_todo).

## The programming model

If you're unfamiliar with Event Sourcing, you can read this first: [Event Sourcing from the ground up, with Ruby examples](https://ismaelcelis.com/posts/event-sourcing-ruby-examples)
For a high-level overview of the mental model, [read this](https://ismaelcelis.com/posts/2025-04-give-it-time/). Or the video version, [here](https://www.youtube.com/watch?v=EgUwnzUJHMA).

The entire behaviour of an event-sourced app is described via **commands**, **events** and **reactions**.

<img width="1024" height="469" alt="sourced-arch-diagram" src="https://github.com/user-attachments/assets/ed916471-525f-4743-bc9a-10a2b6d9f8e9" />


* **Commands** are _intents_ to effect some change in the state of the system. Ex. `Add cart item`, `Place order`, `Update email`, etc.
* **Events** are produced after handling a command and they describe _facts_ or state changes in the system. Ex. `Item added to cart`, `order placed`, `email updated`. Events are stored and you can use them to build views ("projections"), caches and reports to support UIs, or other artifacts.
* **Reactions** are blocks of code that run _after_ an event has been processed and can dispatch new commands in a workflow or automation.
* **State** is whatever object you need to hold the current state of a part of the system. It's usually derived from past events, and it's just enough to interrogate the state of the system and make the next decision.

## Actors

### Overview

Actors are classes that encapsulate the full life-cycle of a concept in your domain, backed by an event stream. This includes loading state from past events and handling commands for a part of your system. They can also define reactions to their own events, or events emitted by other actors. This is a simple shopping cart actor.

```ruby
class Cart < Sourced::Actor
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
  
  # Optionally, define how this actor reacts to the event above.
  # .reaction blocks can dispatch new commands that will be routed to their handlers.
  # This allows you to build workflows.
  reaction :item_added do |event|
    # Evaluate whether we should dispatch the next command.
    # Here we could fetch some external data or query that might be needed
    # to populate the new commands.
    # Here we dispatch a command to the same stream_id present in the event
    dispatch(:send_admin_email, product_id: event.payload.product_id)
  end
  
  # Handle the :send_admin_email dispatched by the reaction above
  command :send_admin_email, product_id: String do |cart, cmd|
    # maybe produce new events
  end
end
```

Using the `Cart` actor in an IRB console. This will use Sourced's in-memory backend by default.

```ruby
cart = Cart.new(id: 'test-cart')
cart.state.total # => 0
# Instantiate a command and handle it
cmd = Cart::AddItem.build('test-cart', product_id: 'p123', price: 1000, quantity: 2)
events = cart.decide(cmd)
# => [Cart::ItemAdded.new(...)]
cmd.valid? # true
# Inspect state
cart.state.total # 2000
cart.items.items.size # 1
# Inspect that events were stored
cart.seq # 2 the sequence number or "version" in storage. Ie. how many commands / events exist for this cart
# Append new messages to the backend
Sourced.config.backend.append_to_stream('test-cart', events)
# Load events for cart
events = Sourced.history_for(cart)
# => an array with instances of [Cart::AddItem, Cart::ItemAdded]
events.map(&:type) # ['cart.add_item', 'cart.item_added']
```

Try loading a new cart instance from recorded events

```ruby
cart2, events = Sourced.load(Cart, 'test-cart')
cart2.seq # 2
cart2.state.total # 2000
cart2.state.items.size # 1
```

### Registering actors

Invoking commands directly on an actor instance works in an IRB console or a synchronous-only web handler, but for actors to be available to background workers, and to react to other actor's events, you need to register them.

```ruby
Sourced.register(Cart)
```

This achieves two things:

1. Messages can be routed to this actor by background processes, using `Sourced.dispatch(message)`.
2. The actor can _react_ to other events in the system (more on event choreography later), via its low-level `.handle(event)` [Reactor Interface](#the-reactor-interface).

These two properties are what enables asynchronous, eventually-consistent systems in Sourced.

### Expanded message syntax

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
  
  ## Now define command and event handlers in a Actor
  class Cart < Sourced::Actor
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

### `.command` block

The class-level `.command` block defines a _command handler_. Its job is to take a command (from a user, an automation, etc), validate it, and apply state changes by publishing new events.

<img width="615" height="168" alt="sourced-command-handler" src="https://github.com/user-attachments/assets/4db26fa1-6671-4611-b994-b3e864cd88b4" />


```ruby
command AddItem do |cart, cmd|
  # logic here...
  # apply and publish one or more new events
  # using instance-level #event(event_type, **payload)
  event ItemAdded, product_id: cmd.payload.product_id
end
```



### `.event` block

The class-level `.event` block registers an _event handler_ used to _evolve_ the actor's internal state.

These blocks are used both to load the initial state when handling a command, and to apply new events to the state in command handlers.

<img width="573" height="146" alt="sourced-evolve-handler" src="https://github.com/user-attachments/assets/174fb8d0-e2ef-41f3-8f43-c94b766529ec" />


```ruby
event ItemAdded do |cart, event|
  cart.items << CartItem.new(**event.payload.to_h)
end
```

These handlers are pure: given the same state and event, they should always update the state in the same exact way. They should never reach out to the outside (API calls, current time, etc), and they should never run validations. They work on events already committed to history, which by definition are assumed to be valid.

### `.before_evolve` block

The class-level `.before_evolve` block registers a callback that runs **before** each registered event handler during state evolution. This is useful for common logic that should run before all event handlers, such as updating timestamps or recording metadata.

```ruby
class CartListings < Sourced::Projector::StateStored
  state do |id|
    { id:, items: [], updated_at: nil, seq: 0 }
  end

  # This block runs before any .event handler
  before_evolve do |state, event|
    state[:updated_at] = event.created_at
    state[:seq] = event.seq
  end

  event Cart::ItemAdded do |state, event|
    state[:items] << event.payload.to_h
  end

  event Cart::Placed do |state, event|
    state[:status] = :placed
  end
end
```

The `before_evolve` callback only runs for events that have a registered handler via the `.event` macro. If an event is not handled by this class, the callback is skipped for that event.

### `.reaction` block

The class-level `.reaction` block registers an event handler that _reacts_ to events already published by this or other Actors.

`.reaction` blocks can dispatch the next command in a workflow with the instance-level `#dispatch` helper.

<img width="504" height="109" alt="sourced-react-handler" src="https://github.com/user-attachments/assets/b181ebdd-4bc7-4692-a2ab-910c1a829ec4" />


```ruby
reaction ItemAdded do |cart, event|
  # dispatch the next command to the event's stream_id
  dispatch(
    CheckInventory, 
    product_id: event.payload.product_id,
    quantity: event.payload.quantity
  )
end
```

You can also dispatch commanda to _other_ streams. For example for starting concurrent workflows.

```ruby
# dispatch a command to a new custom-made stream_id
dispatch(CheckInventory, event.payload).to("cart-#{Time.now.to_i}")

# Or use Sourced.new_stream_id
dispatch(CheckInventory, event.payload).to(Sourced.new_stream_id)

# Or start a new stream and dispatch commands to another actor
dispatch(:notify, message: 'hello!').to(NotifierActor)
```

#### `.reaction` block with actor state

 `.reaction`  blocks receive the actor state, which is derived by applying past events to it (same as when handling commands).

```ruby
# Define an event handler to evolve state
event ItemAdded do |state, event|
  state[:item_count] += 1
end

# Now react to it and check state
reaction ItemAdded do |state, event|
  if state[:item_count] > 30
    dispatch NotifyBigCart
  end
end
```

#### `.reaction` with state for all events

If the event name or class is omitted, the `.reaction` macro registers reaction handlers for all events already registered for the actor with the `.event` macro, minus events that have specific reaction handlers defined.

```ruby
# wildcard reaction for all evolved events
reaction do |state, event|
  if state[:item_count] > 30
    dispatch NotifyBigCart
  end
end
```

#### `.reaction` for multiple events

```ruby
reaction ItemAdded, InventoryChecked do |state, event|
  # etc
end
```

It also works with symbols, for messages that have been defined as symbols (ex `event :item_added`)

```ruby
reaction :item_added, InventoryChecked do |state, event|
  # etc
end
```

## Causation and correlation

When a command produces events, or when an event makes a reactor dispatch a new command, the cause-and-effect relationship between these messages is tracked by Sourced in the form of `correlation_id` and `causation_id` properties in each message's metadata.

<img width="878" height="326" alt="sourced-causation-correlation" src="https://github.com/user-attachments/assets/88d86b65-50ff-4222-8941-406826fab243" />


This helps the system keep a full audit trail of the cause-and-effect behaviour of the entire system.

<img width="877" height="629" alt="CleanShot 2025-11-11 at 23 59 40" src="https://github.com/user-attachments/assets/38765370-2e80-46d8-bf30-1651208d5cf9" />

## Background vs. foreground execution

TODO

## Projectors

Projectors react to events published by actors and update views, search indices, caches, or other representations of current state useful to the app. They can both react to events as they happen in the system, and also "catch up" to past events. Sourced keeps track of where in the global event stream each projector is.

From the outside-in, projectors are classes that implement the _Reactor interface_.

Sourced ships with two ready-to-use projectors, but you can also build your own.

### State-stored projector

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
  sync do |state:, events:, replaying:|
    state.save!
  end
end
```

### Event-sourced projector

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
  sync do |state:, events:, replaying:|
    File.write("/listings/#{state[:id]}.json", JSON.dump(state)) 
  end
end
```

### Registering projectors

Like any other _reactor_, projectors need to be registered for background workers to route events to them.

```ruby
# In your app's configuration
Sourced.register(CartListings)
```

### Reacting to events and scheduling the next command from projectors

Sourced projectors can define `.reaction` handlers that will be called after evolving state via their `.event` handlers, in the same transaction.

This can be useful to implement TODO List patterns where a projector persists projected data, and then reacts to the data update using the data to schedule the next command in a workflow.

![CleanShot 2025-05-30 at 18 43 01](https://github.com/user-attachments/assets/ef8a61b7-6b99-49a1-9767-af94b9c2c4e2)


```ruby
class ReadyOrders < Sourced::Projector::StateStored
  # Fetch listing record from DB, or new one.
  state do |id|
    OrderListing.find_or_initialize(id)
  end

  event Orders::ItemAdded do |listing, event|
    listing.line_items << event.payload
  end
  
  # Evolve listing record from events
  event Orders::PaymentConfirmed do |listing, event|
    listing.payment_confirmed = true
  end

  event Orders::BuildConfirmed do |listing, event|
    listing.build_confirmed = true
  end
  
  # Sync listing record back to DB
  sync do |state:, events:, replaying:|
    state.save!
  end
  
  # If a listing has both the build and payment confirmed,
  # automate dispatching the next command in the workflow
  reaction do |listing, event|
    if listing.payment_confirmed? && listing.build_confirmed?
      dispatch Orders::Release, **listing.attributes
    end
  end
end
```

Projectors can also define `.reaction event_class do |state, event|` to react to specific events, or `reaction event1, event2` to react to more than one event with the same block.

### Skipping projector reactions when replaying events

When a projector's offsets are reset (so that it starts re-processing events and re- building projections), Sourced skips invoking a projector's `.reaction` handlers. This is because building projections should be deterministic, and rebuilding them should not trigger side-effects such as automations (we don't want to call 3rd party APIs, send emails, or just dispatch the same commands over and over when rebuilding projections).

To do this, Sourced keeps track of each consumer groups' highest acknowledged event sequence. When a consumer group is reset and starts re-processing past events, this sequence number is compared with each event's sequence, which tells us whether the event has been processed before.

## Concurrency model

Concurrency in Sourced is achieved by explicitely _modeling it in_.

Sourced workers process messages by acquiring locks on `[reactor group ID][stream ID]`. For example `"CartActor:cart-123"`

This means that all events for a given reactor/stream are processed in order, but events for different streams can be processed concurrently. You can define workflows where some work is done concurrently by modeling them as a collaboration of streams.

### Single-stream sequential execution

In the following (simplified!) example, a Holiday Booking workflow is modelled as a single stream ("Actor"). The infrastructure makes sure these steps are run sequentially.

<img width="1583" height="292" alt="sourced-concurrency-single-lane" src="https://github.com/user-attachments/assets/025529de-906c-41b4-8f21-0b7759b6e394" />

The Actor glues its steps together by reacting to events emitted by the previous step, and dispatching the next command.

```ruby
class HolidayBooking < Sourced::Actor
  # State and details omitted...
  
  command :start_booking do |state, cmd|
    event :booking_started
  end
  
  reaction :booking_started do |event|
    dispatch :book_flight
  end
  
  command :book_flight do |state, cmd|
    event :flght_booked
  end
  
  reaction :flight_booked do |event|
    dispatch :book_hotel
  end
  
  command :book_hotel do |state, cmd|
    event :hotel_booked
  end
  
  # Define event handlers if you haven't...
  event :booking_started, # ..etc
  event :flight_booked, # ..etc
end
```

### Multi-stream concurrent execution

In this other example, the same workflow is split into separate streams/actors, so that Flight and Hotel bookings can run concurrently from each other. When completed, they each notify the parent Holiday actor, so the whole process coalesces into a sequential operation again.

<img width="1787" alt="sourced-concurrency-multi-lane" src="https://github.com/user-attachments/assets/444445ff-b837-4c19-8c28-1b47eada7a41" />

```ruby
# An actor dispatches a message to different stream
# messages for different streams are processed concurrently
reaction BookingStarted do |state, event|
  dispatch(BookHotel).to("#{event.stream_id}-hotel")
end
```

### Units of work

<img width="1249" height="652" alt="CleanShot 2025-11-15 at 14 38 05" src="https://github.com/user-attachments/assets/a3631dd5-08b9-4381-8082-ce5cdc8958ed" />

The diagram shows the units of work in an example Sourced workflow. The operations within each of the red boxes are protected by a combination of transactions and locking strategies on the consumer group + stream ID, so they are isolated from other concurrent processing. They can be said to be **immediately consistent**. 
The data-flow _between_ these boxes is propagated asynchronously by Sourced's infrastructure so, relative to each other, the entire system is **eventually consistent**.

These transactional boundaries are guarded by the same locks that enforce the concurrency model, so that for example the same message can't be processed twice by the same Reactor (workflow, projector, etc). 

## Durable workflows

There's a `Sourced::DurableWorkflow` class that can be subclassed to define Reactors with a synchronous-looking API. This is *work in progress*.

```ruby
class BookHoliday < Sourced::DurableWorkflow
  # This method can be called like a regular method
  # The methods inside also have blocking semantics
  # but they're in fact event-sourced, and will be
  # retried on failure until the booking completes.
  # Methods that were succesful will be idempotent on retry
  def execute(flight_info, hotel_info)
    flight = book_flight(flight_info)
    hotel = book_hotel(hotel_info)
    confirm_booking(flight, hotel)
  end
  
  # The .durable macro turns a regular method
  # into an event-sourced workflow
  durable def book_flight(info)
    FlightsAPI.book(info)
  end
  
  durable def book_hotel(info)
    HotelsAPI.book(info)
  end
  
  durable def confirm_booking(flight, hotel)
    # etc,
  end
end
```

These executions will be handed off to the runtime to be run by one or more workers, while preserving ordering. You can optionally wait for a result.

```ruby
result = BookHoliday.execute(flight_info, hotel_info).wait.output
# Confirmed booking, or whatever error result your code returns
```

Events for the full execution are recorded to the backend.
<img width="1016" height="1298" alt="CleanShot 2025-11-13 at 13 48 27@2x" src="https://github.com/user-attachments/assets/a591a1a4-88e6-435e-bb27-cb4990aaf91f" />

Durable workflows must be registered with the runtime, like any other Reactor.

```ruby
Sourced.register BookHoliday
```

## Handler DSL

The `Sourced::Handler` mixin provides a lighter-weight DSL for simple reactors.

```ruby
class OrderTelemetry
  include Sourced::Handler
  
  # Handle these Order events
  # and log them
  on Order::Started do |event|
    Logger.info ['order started', event.stream_id]
    []
  end
  
  on Order::Placed do |event|
    Logger.info ['order placed', event.stream_id]
    []
  end
end

# Register it
Sourced.register OrderTelemetry
```

Handlers can optionally define the `:history` argument. The runtime will provide the full message history for the stream ID being handled.

```ruby
on Order::Placed do |event, history:|
  total = history
    .filter { |e| Order::ProductAdded === e }
    .reduce(0) { |n, e| n + e.payload.price }
  
  if total > 10000
    return [Order::AddDiscount.build(event.stream_id, amount: 100)]
  end
  
  []
end
```

It also supports multiple event types, for generic handling.

```ruby
on Order::Placed, Order::Complete do |event|
  Logger.info "received event #{event.inspect}"
  []
end
```

## Command methods for Actors

The optional `Sourced::CommandMethods` mixin allows invoking an Actor's commands as regular methods.

`CommandMethods` automatically generates instance methods from command definitions,
allowing you to invoke commands in two ways:

1. **In-memory version** (e.g., `actor.start(name: 'Joe')`)
   - Validates the command and executes the decision handler
   - Returns a tuple of [cmd, new_events]
   - Does NOT persist events to backend
2. **Durable version** (e.g., `actor.start!(name: 'Joe')`)
   - Same as in-memory, but also appends events to backend
   - Raises `FailedToAppendMessagesError` if backend fails

Include the module in an Actor and define commands normally:

```ruby
class MyActor < Sourced::Actor
  include Sourced::CommandMethods

  command :create_item, name: String do |state, cmd|
    event :item_created, cmd.payload
  end
end

actor = MyActor.new(id: 'actor-123')
cmd, events = actor.create_item(name: 'Widget')  # In-memory
cmd, events = actor.create_item!(name: 'Widget') # Persists to backend
```



## Orchestration and choreography

### Orchestration

Orchestration is when the flow control of a multi-collaborator workflow is centralised into a single entity. This can be achieved by having one Actor coordinate the communication by reacting to events and sending commands to other actors.

```ruby
class HolidayBooking < Sourced::Actor
  state do |id|
    BookingState.new(id)
  end
  
  command StartBooking do |booking, cmd|
    # validations, etc
    event BookingStarted, cmd.payload
  end
  
  event BookingStarted
  
  # React to BookingStarted and start sub-workflows
  reaction BookingStarted do |booking, event|
    dispatch(HotelBooking::Start)
  end
  
  # React to events emitted by sub-workflows
  reaction HotelBooking::Started do |booking, event|
    dispatch(ConfirmHotelBooking, event.payload)
  end
  
  command ConfirmHotelBooking do |booking, cmd|
    unless booking.hotel.booked?
      event HotelBookingConfirmed, cmd.payload
    end
  end
  
  event HotelBookingConfirmed do |booking, event|
    # update booking state
    booking.confirm_hotel(event.payload)
  end
end
```

This is a verbose step-by-step choreography, but it can be made more succint by ommiting the mirroring of commands/events, if needed (or by using the [Reactor Interface](#the-reactor-interface) directly).

*TODO*: a way for Actors to initialise their internal state with event attributes other than the `stream_id`. For example, events may carry a `booking_id` for the overall workflow.

### Choreography

Choreography is when each component reacts to other components' events without centralised control. The overall workflow "emerges" from this collaboration.

```ruby
class HotelBooking < Sourced::Actor
  # The HotelBooking defines its own
  # reactions to booking events
  reaction HolidayBooking::StartBooking do |state, event|
    # dispatch a command to itself to start its own life-cycle
    dispatch Start, event.payload
  end
  
  command Start do |state, cmd|
    # validations, etc
    # other Actors in the choreography
    # can choose to react to events emitted here
    event Started, cmd.payload
  end
  
  event Started do |state, event|
    # update state, etc
  end
end
```

## Appending and reading messages

### Appending messages without optimistic locking

Use `Backend#append_next_to_stream` to append messages to a stream, with no questions asked.

```ruby
message = ProductAdded.build('order-123', product_id: 123, price: 100)
Sourced.config.backend.append_next_to_stream('order-123', [message])

# Shortcut:
Sourced.dispatch(message)
```

### Appending messages with optimistic locking

Using `Backend#append_to_stream`, the backend expects the new messages `seq` property (sequence number) to be greater than the last message in storage for the same stream. This is to catch concurrent writes where a different client or thread may append to the stream while your code was preparing for it.

```ruby
# Your code must make sure to increment sequence numbers
past_events = Sourced.config.backend.read_stream('order-123')
last_known_seq = past_events.last&.seq # ex. 10
# Instantiate new messages and make sure to increment their sequences
message = ProductAdded.new(
  stream_id: 'order-123', 
  seq: last_known_seq + 1, # <== incremented sequence
  payload: { product_id: 123, price: 100 }
)

# This will raise an exception if there's already a message
# for this stream with this sequence number in storage.
Sourced.backend.append_to_stream('order-123', [message])
```

`Sourced::Actor` classes do this incrementing automatically when they produce new messages.

### Scheduling messages in the future

You can append messages to a separate log, with a schedule time. Sourced workers will periodically poll this log and move these messages into the main log at the right time.

```ruby
message = ProductAdded.build('order-123', product_id: 123, price: 100)
Sourced.config.backend.schedule_messages([message], at: Time.now + 20)
```

Actor reactions can use the `#dispatch` and `#at` helpers to schedule commands to run at a future time.

```ruby
reaction ProductAdded do |order, event|
  dispatch(NotifyNewProduct).at(Time.now + 20)
end
```

## Replaying messages

You can use the backend API to reset offsets for a specific consumer group, which will cause workers to start replaying messages for that group.

```ruby
Sourced.config.backend.reset_consumer_group(ReadyOrder)
```

See [below](#stopping-and-starting-consumer-groups) for other consumer lifecycle methods.	

## The Reactor Interface

All built-in Reactors (Actors, Projections) build on the low-level Reactor Interface

```ruby
class MyReactor
  extend Sourced::Consumer
  
  # The runtime will poll and hand over messages of this type
  # to this class' .handle() method
  def self.handled_messages = [Order::Started, Order::Placed]
  
  # The runtime invokes this method when it finds a new message
  # of type present in the list above
  def self.handle(new_message)
    # Process message here.
    # This method can return an Array or one or more of the following
    actions = []
    
    # Just aknowledge new_message
    actions << Sourced::Actions::OK
    
    # Append these new messages to the event store
    # Sourced will automatically increment the stream's sequence number
    # (ie. no optimistic locking)
    started = Order::Started.build(new_message.stream_id)
    actions << Sourced::Actions::AppendNext.new([started])
    
    # Append these new messages to the event store.
    # The messages are expected to have a :seq incremented after new_message.seq
    # Messages will fail to append if other messages have been appended
    # with overlapping sequence numbers (optimistic locking)
    started = Order::Started.new(stream_id: new_message.stream_id, seq: new_message.seq + 1)
    actions << Sourced::Actions::AppendAfter.new(new_message.stream_id, [started])
    
    # Tell the runtime to retry this message
    # This is a low-level action and Sourced already uses it when handling exceptions
    # and retries
    actions << Sourded::Actions::RETRY
    
    actions
  end
end
```

You can implement your own low-level reactors following the interface above. Then register them as normal.

```ruby
Sourced.register MyReactor
```

### Reactors that require message history

Reactors that declare the `:history` argument will also be provided the full message history for the stream being handled.

This is how event-sourced Actors are implemented.

```ruby
def self.handle(new_message, history:)
  # evolve state from history,
  # handle command, return new events, etc
  []
end
```

### `:replaying` flag.

Your `.handle` method can also declare a `:replaying` boolean, which tells the reactor whether the stream is replaying events, or handling new messages. Reactors use this to run or omit side-effects (for example, replaying Projectors don't run `reaction` blocks).

```ruby
def self.handle(new_message, history:, replaying:)
  if replaying
    # Omit side-effects
  else
    # Trigger side-effects
  end
end
```

## Testing

There's a couple of experimental RSpec helpers that allow testing Sourced reactors in GIVEN, WHEN, THEN style.

*GIVEN* existing events A, B, C
WHEN new command D is sent
THEN I expect new events E and F

### Single reactor

Use `with_reactor` to unit-test the life-cycle of a single reactor.

```ruby
require 'sourced/testing/rspec'

RSpec.describe Order do
  include Sourced::Testing::RSpec

  it 'adds product to order' do
    with_reactor(Order, 'order-123')
      .when(Order::AddProduct, product_id: 1, price: 100)
      .then(Order::ProductAdded.build('order-123', product_id: 1, price: 100))
  end

  it 'is a noop if product already in order' do
    with_reactor(Order, 'order-123')
      .given(Order::ProductAdded, product_id: 1, price: 100)
      .when(Order::AddProduct, product_id: 1, price: 100)
      .then([])
  end
end
```

`#then` can also take a block, which will be given the low level `Sourced::Actions` objects returned by your `.handle()` interface.

You can use this block to test reactors that trigger side effects.

```ruby
with_reactor(Webhooks, 'webhook-1')
  .when(Webooks::Dispatch, name: 'Joe')
  .then do |actions|
    expect(api_request).to have_been_requested
  end
```

You can mix argument and block assertions with `.then()`

```ruby
with_reactor(Webhooks, 'webhook-1')
  .when(Webooks::Dispatch, name: 'Joe')
  .then do |_|
    expect(api_request).to have_been_requested
  end
  .then(Webhooks::Dispatched, reference: 'webhook-abc')
```

For reactors that have `sync` blocks for side-effects (ex. Projectors), use `#then!` to trigger those side-effects and assert their results.

```ruby
with_reactor(PlacedOrders, 'order-123')
  .given(Order::Started)
  .given(Order::ProductAdded, product_id: 1, price: 100, units: 2)
  .given(Order::Placed)
  .then! do |_|
    expect(OrderRecord.find('order-123').total).to eq(200)
  end
```

### Multiple reactors (A.K.A "Sagas")

Use `with_reactors` to test the collaboration of multiple reactors sending and picking up eachother's messages.

```ruby
it 'tests collaboration of reactors' do
  order_stream = 'actor-1'
  payment_stream = 'actor-1-payment'
  telemetry_stream = Testing::Telemetry::STREAM_ID

  # With these reactors
  with_reactors(Order, Payment, Telemetry)
    # GIVEN that these events exist in history
    .given(Order::Started.build(order_stream, name: 'foo'))
    # WHEN I dispatch this new command
    .when(Order::StartPayment.build(order_stream))
    # Then I expect
    .then do |stage|
      # The different reactors collaborated and
      # left this message trail behind
      # Backend#messages is only available in the TestBackend
      expect(stage.backend.messages).to match_sourced_messages([
        Order::Started.build(order_stream, name: 'foo'), 
        Order::StartPayment.build(order_stream), 
        Order::PaymentStarted.build(order_stream), 
        Telemetry::Logged.build(telemetry_stream, source_stream: order_stream),
        Payment::Process.build(payment_stream), 
        Payment::Processed.build(payment_stream),
        Telemetry::Logged.build(telemetry_stream, source_stream: payment_stream),
      ])
    end
end
```

`with_reactors` sets up its own in-memory backend, so you can test multi-reactor workflows in terms of what messages they produce without database or network requests, and there's no need for database setup or tear-down. Just test the behaviour!

The `.then` block can take an optional second argument, which will be passed as only the _new_ messages produced by the reactors, appended after any messages setup with `given`.

```ruby
.then do |stage, new_messages|
  expect(new_messages).to match_sourced_messages([...])
end
```



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

  # Worker and housekeeping options (shown with defaults)
  config.worker_count = 2                       # Number of worker fibers
  config.housekeeping_count = 1                 # Number of housekeeper fibers
  config.housekeeping_interval = 3              # Seconds between scheduling cycles
  config.housekeeping_heartbeat_interval = 5    # Seconds between worker heartbeats
  config.housekeeping_claim_ttl_seconds = 120   # Seconds before stale claims are reaped
end

Sourced.config.backend.install unless Sourced.config.backend.installed?
```

These options are used by both `Sourced::Supervisor` and the Falcon integration. When running workers alongside a web server (Falcon, or any other Async-compatible server), these control how many worker and housekeeper fibers are spawned per OS process.

### Generating Sequel migrations

If your app already uses Sequel's migrator, you can copy Sourced's migration into your migrations directory instead of using `backend.install`.

```ruby
backend = Sourced.config.backend
backend.copy_migration_to("db/migrations")
# => writes db/migrations/001_create_sourced_tables.rb
```

Or use a block to control the file name (e.g. timestamped migrations):

```ruby
backend.copy_migration_to do
  "db/migrations/#{Time.now.strftime('%Y%m%d%H%M%S')}_create_sourced_tables.rb"
end
```

The generated file is a standard `Sequel.migration { change { ... } }` that works with `Sequel::Migrator`. It respects the `prefix` and `schema` options passed when configuring the backend:

```ruby
Sourced.configure do |config|
  db = Sequel.connect(ENV.fetch('DATABASE_URL'))
  config.backend = Sourced::Backends::SequelBackend.new(db, prefix: 'myapp', schema: 'events')
end

# Migration will create tables like events.myapp_messages, events.myapp_streams, etc.
Sourced.config.backend.copy_migration_to("db/migrations")
```

Register your Actors and Reactors.

```ruby
Sourced.register(Leads::Actor)
Sourced.register(Leads::Listings)
Sourced.register(Webooks::Dispatcher)
```

### Running workers as a separate process

When using a web server that doesn't share Sourced's Async event loop (e.g. Puma), or in non-web applications, run workers as a standalone process using `Sourced::Supervisor`:

```ruby
# worker.rb
require_relative 'config/environment'
Sourced::Supervisor.start
```

This requires managing two processes in deployment: one for your web server, one for workers.

### Running workers with Falcon

If you use [Falcon](https://github.com/socketry/falcon) as your web server, you can run Sourced workers in the same process. Both Falcon and Sourced use the [Async](https://github.com/socketry/async) gem, so workers run as lightweight fibers alongside web requests — no separate worker process needed.

Add `sourced/falcon` to your setup (no hard dependency on Falcon in sourced.gemspec):

```ruby
# falcon.rb
#!/usr/bin/env falcon-host
require 'bundler/setup'
require 'sourced/falcon'
require_relative 'config/environment' # app setup, Sourced.configure, register reactors, etc.

service "my-app" do
  include Sourced::Falcon::Environment
  include Falcon::Environment::Rackup    # loads config.ru

  # -- Falcon / Async options --
  url "http://[::]:9292"                 # Server bind URL (default: "http://[::]:9292")
  count 2                                # Number of OS processes to fork (default: Etc.nprocessors)
  timeout 30                             # Connection timeout in seconds (default: nil)
  verbose false                          # Enable verbose logging (default: false)
  cache true                             # Enable HTTP response caching (default: false)

  # Sourced worker options default to Sourced.config values.
  # Override per-service if needed:
  # sourced_worker_count 4
  # sourced_housekeeping_count 1
  # sourced_housekeeping_interval 3
  # sourced_housekeeping_heartbeat_interval 5
  # sourced_housekeeping_claim_ttl_seconds 120
end
```

Run with:

```
falcon host
```

Total Sourced workers = `count * sourced_worker_count`. For example, `count 2` and `sourced_worker_count 4` gives 8 worker fibers across 2 OS processes, all competing for events via database locks (same as running multiple Supervisors).

On shutdown (`Ctrl-C` / `SIGTERM`), Falcon signals workers to stop. Their poll loops exit gracefully with no stale claims.

## Custom attribute types and coercions.

Define a module to hold your attribute types using [Plumb](https://github.com/ismasan/plumb)

```ruby
module Types
  include Plumb::Types
  
  # Your own types here.
  CorporateEmail = Email[/@apple\.com^/]
end
```

Then you can use any [built-in Plumb types](https://github.com/ismasan/plumb?tab=readme-ov-file#built-in-types), as well as your own, when defining command or event structs (or any other data structures for your app).

```ruby
UpdateEmail = Sourced::Command.define('accounts.update_email') do
  attribute :email, Types::CorporateEmail
end
```

## Error handling

Sourced workflows are eventually-consistent by default. This means that commands and events are handled in background processes, and any exceptions raised can't be immediatly surfaced back to the user (and, there might not be a user anyway!).

Most "domain errors" in command handlers should be handled by the developer and recorded as domain events, so that the domain can react and/or compensate for them.

To handle true _exceptions_ (code or data bugs, network or IO exceptions) Sourced provides a default error strategy that will "stop" the affected consumer group (the Postgres backend will log the exception and offending message in the `consumer_groups` table).

You can configure the error strategy with retries and exponential backoff, as well as `on_retry` and `on_stop` callbacks.

```ruby
Sourced.configure do |config|
  # config.backend = Sequel.connect(ENV.fetch('DATABASE_URL'))
  config.error_strategy do |s|
    s.retry(
      # Retry up to 3 times
      times: 3,
      # Wait 5 seconds before retrying
      after: 5, 
      # Custom backoff: given after=5, retries in 5, 10 and 15 seconds before stopping
      backoff: ->(retry_after, retry_count) { retry_after * retry_count }
    )
    
    # Trigger this callback on each retry
    s.on_retry do |n, exception, message, later|
      LOGGER.info("Retrying #{n} times")
    end

    # Finally, trigger this callback
    # after all retries have failed and the consumer group is stopped.
    s.on_stop do |exception, message|
      Sentry.capture_exception(exception)
    end
  end
end
```

### Custom error strategy

You can also configure your own error strategy. It must respond to `#call(exception, message, group)`

```ruby
CUSTOM_STRATEGY = proc do |exception, message, group|
  case exception
  when Faraday::Error
    group.retry(Time.now + 10)
  else
    group.stop(exception)
  end
end

Sourced.configure do |config|
  # Configure backend, etc
  config.error_strategy = CUSTOM_STRATEGY
end
```

## Stopping and starting consumer groups.

`Sourced.config.backend` provides an API for stopping and starting consumer groups. For example to resume groups that were stopped by raised exceptions, after the error has been corrected.

```ruby
Sourced.config.backend.stop_consumer_group('Carts::Listings')
Sourced.config.backend.start_consumer_group('Carts::Listings')
```

## Rails integration

Soon.

## Sourced vs. ActiveJob

ActiveJob is a great way to handle background jobs in Rails. It's simple and easy to use. However, it's not designed for event sourcing.
ActiveJob backends (and other job queues) are optimised for parallel processing of jobs, this means that multiple jobs for the same business entity may be processed in parallel without any ordering guarantees.

<img width="832" height="493" alt="sourced-job-queue-diagram" src="https://github.com/user-attachments/assets/c51b03be-8794-4954-968a-87ecdd97d2f7" />

Sourced's concurrency model is designed to process events for the same entity in order, while allowing for parallel processing of events for different entities.

<img width="802" height="552" alt="sourced-ordered-streams-diagram" src="https://github.com/user-attachments/assets/ddfbff4b-11bb-4e0c-93e9-e0851c4721d9" />

## Gotchas

Currently `Sourced` is focused on eventual consistency and background processing
of commands and events through background workers. Eventually a synchronous mode
will be added for simpler use-cases.

This can be confusing if you expect your reactions to run automatically and
synchronously when you issue commands.

This can be a gotcha if you're using the `Sourced::CommandMethods` mixin which
persists events but does not call your reactor right away. If you need to you
should explicitly call `#react` after issuing commands.

```ruby
```
```ruby
chat = Sourced.load(Chat, 'chat-123')
# Would persist but not call reactions
_cmd, events = chat.send_message!(content: query)
# Have to react manually
commands = chat.react(events)
# now dispatch these commands again?
```


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

Bug reports and pull requests are welcome on GitHub at https://github.com/ismasan/sourced.	
