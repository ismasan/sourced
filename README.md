# Sourced

[![Build Status](https://travis-ci.org/ismasan/sourced.svg?branch=master)](https://travis-ci.org/ismasan/sourced)

Bare-bones Event Sourcing in Ruby. WiP.

![diagram](https://static.swimlanes.io/ab58ca5bacf8a6e60024c3e8335bdfee.png)

## TL;DR;

This gem attempts to provide the basic components to build in-process event-sourced apps in Ruby.

### Entities

An entity represents the current state of an object in your domain. It can be any type (a Hash, a Struct, your own class, etc) as long as it exposes an `id`.
Entities are sometimes called "aggregate root" in Event Sourcing circles, but in reality an entity does not have to qualify as an [aggregate](https://martinfowler.com/bliki/DDD_Aggregate.html) object. Any object will do.

```ruby
User = Struct.new(:id, :name, :age, keyword_init: true)
```

### Events

Events describe things that have happened to entities in your system, and are usually produced in response to commands of some kind.
For example, a `CreateUser` command might result in a `UserCreated` event.
Events are named in past tense. ex. "user created", "account updated".

```ruby
UserCreated = Sourced::Event.define('users.created') do
  property :name, Sourced::Types::String
  property :age, Sourced::Types::Integer
end
UserNameUpdated = Sourced::Event.define('users.updated.name') do
  property :name, Sourced::Types::String
end
UserAgeUpdated = Sourced::Event.define('users.updated.age') do
  property :age, Sourced::Types::Integer
end
```

* Sourced uses [Dry-Types](https://dry-rb.org/gems/dry-types/1.2/) for event property definitions.

Events are inmutable struct definitions.
Events are assumed to be valid. Validating user input or domain data should be done at the command layer, which will depend on your app.

All Sourced events come with a basic data schema.

```ruby
topic # String, required. Ex. 'users.created'
id # UUID, required, set on creation
entity_id # UUID, required
date # Time, set on creation
seq # Integer, usually set by stages (more on that below)
originator_id # UUID, optional. The command or event that lead up to this event.
payload # Object, your custom event properties.
```

You add field definitions to event constructors by passing a block to `Sourced::Event.define(topic, &block)`.

#### Instantiating events

You can build an instance of a given event class:

```ruby
evt = UserCreated.new(entity_id: Sourced.uuid, payload: { name: 'Joan', age: 38 })
evt.id # UUID
evt.entity_id # UUID
evt.payload.name # 'Joan'
evt.payload.age # 38
```

You can build events of the right class from a hash (uses `topic` to find class).
This is used for deserializing from storage.

```ruby
# Will return a UserCreated event
evt = Sourced::Event.from(
  topic: 'users.created',
  payload: {
    name: 'Joan',
    age: 38
  }
)
```

### Projectors

A projector subscribes to events and uses them to mutate entities (*).

```ruby
class UserProjector < Sourced::Projector
  on UserCreated do |user, evt|
    user.id = evt.payload.id
    user.name = evt.payload.name
    user.age = evt.payload.age
  end

  on UserNameUpdated do |user, evt|
    user.name = evt.payload.name
  end

  on UserAgeUpdated do |user, evt|
    user.age = evt.payload.age
  end
end
```

A projector produces a simple callable object that applies given events to an entity instance.

```ruby
user_id = Sourced.uuid
user = User.new(name: nil, age: nil)
projector = UserProjector.new
projector.call(user, UserCreated.new(entity_id: user_id, payload: { name: 'Joe', age: 40 }))
projector.call(user, UserAgeUpdated.new(entity_id: user_id, payload: { age: 41 }))
user.name # "Joe"
user.age # 41
```

(*) `Sourced::Projector` assumes Entities to be mutable. An alternative would have been to make projectors pure functions that return copies of entities,
but this makes the syntax less idiomatic, and also means that projector blocks must always return an entity instance, which is easy to forget.

Projectors are just syntax sugar for the following interface: `#call(Entity, Event) Entity`, so you can create your own. Example:

The following is a "pure" functional projector that returns copies of immutable entities.

```ruby
simple_user_projector = proc do |user, evt|
  case evt
    when UserCreated
      user.copy(id: evt.entity_id, **evt.payload)
    when UserNameUpdated
      user.copy(**evt.payload)
    # etc...
    else
      user
  end
end
```

### Stage

A Stage composes an entity factory and a projector into an entity's life-cycle.

```ruby
class UserStage < Sourced::Stage
  # A factory to initialize a new Entity
  # Can be a block, or any `#call(id) Entity` interface.
  entity do |id|
    User.new(id: id)
  end

  # A projector to project events into user entities.
  # Accepts a block (to be wrapped by `Sourced::Projector`),
  # or any `#call(Entity, Event) Entity` interface.
  projector do
    on UserCreated do |user, evt|
      user[:name] = evt.payload.name
      user[:age] = evt.payload.age
    end
    # etc
  end
end
```

The Stage entity life-cycle is:

```ruby
# 1). Given a stream of events _for the same entity_, re-constitute the current state of an entity.
events = [event1, event2, event3]
stage = UserStage.load(event1.entity_id, events)

# 2). Apply new events to the current entity state.
stage.apply(UserAgeChanged, payload: { age: 50 })
stage.apply(UserNameChanged, payload: { name: 'Joan' })

# User entity has been updated
state.entity.name # "Joan"

# Stage#events lists new events applied since last load
stage.events# [<UserAgeChanged>, <UserNameChanged>]

# Stage tracks event sequence number.
stage.events.map(&:seq) # [4, 5]
# Applied events are populated with #entity_id
stage.events.map(&:entity_id)

# #last_committed_seq is the last event sequence loaded
stage.last_committed_seq # 3
# #seq is the current sequence number (from the last event applied)
stage.seq # 5

# 3). Commit new events
# This yields new events for storage
# and resets new event list and sequences only if storage was successful.
stage.commit do |last_committed_seq, applied_events, user|
  # last_committed_seq can be used for optimistic locking
  SomeEventStore.append(applied_events, last_committed_seq)
end

# On successful commit, stage is updated
stage.events # []
stage.last_committed_seq # 5
stage.seq # 5
```

### Event Store

An _Event Store_ persists and retrieves events from storage.
It must implement the following interface (*):

```
# Append events to storage
append(Array<Event>, expected_seq: nil | Integer) Array<Event>
# Retrieve entire list of events for an entity ID
by_entity_id(id UUID, options Hash) Array<Event>
```

Currently Sourced ships with an `Sourced::MemEventStore` (in-memory, for tests) implementation.
Different implementations could be written to support databases, Kafka, file system, etc.

(*) event stores implementations are free to expose othe methods too, for example for filtering or querying events.

### Entity Repo

ToDO
### Command Handler

ToDO

### Event Store

ToDO

### Subscribers

ToDO

### Projections

ToDO

## Using with REST APIs

ToDO

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sourced'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sourced

## Usage

TODO: Write usage instructions here

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/ismasan/sourced.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
