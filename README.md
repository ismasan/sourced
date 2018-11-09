# Sourced

[![Build Status](https://travis-ci.org/ismasan/sourced.svg?branch=master)](https://travis-ci.org/ismasan/sourced)
 
Bare-bones Event Sourcing in Ruby. WiP.

![diagram](https://static.swimlanes.io/ab58ca5bacf8a6e60024c3e8335bdfee.png)

## TL;DR;

This gem gives you the basic components to build in-process event-sourced apps in Ruby. Extend by providing your own implementation of the interfaces described here.

### Commands

Commands are the things that you want your app to do. They describe _intents_, and by convention are named in the imperative (ex. "create user", "update account").

```ruby
CreateUser = Sourced::Event.define('users.create') do
  field(:name).type(:string).present
  field(:age).type(:integer).present
end
```

### Events

Events describe things that have happened in your system, and are usually produced in response to commands.
For example, a `CreateUser` command might result in a `UserCreated` event.
Events are named in past tense. ex. "user created", "account updated".

```ruby
UserCreated = Sourced::Event.define('users.created') do
  field(:name).type(:string).present
  field(:age).type(:integer).present
end
UserNameUpdated = Sourced::Event.define('users.updated.name') do
  field(:name).type(:string).present
end
UserAgeUpdated = Sourced::Event.define('users.updated.age') do
  field(:age).type(:integer).present
end
```

Commands and events define data schemas and their validations. Both are inmutable.
Sourced won't let invalid events go through.
You can add your own validators and field types. See [Parametric](https://github.com/ismasan/parametric) for more.

All Sourced commands and events come with a basic data schema.

```ruby
topic # String, required
id # UUID, required, set on creation
aggregate_id # UUID, required
date # Time, set on creation
version # Integer, usually sey by aggregates (more on that below)
parent_id # UUID, optional. Set by command handlers
```

You add field definitions to event constructors by passing a block to `Sourced::Event.define(topic, &block)`.

#### Instantiating events

You can build an instance of a given event or command class:

```ruby
# this will raise an exception if event data is invalid or missing
cmd = CreateUser.instance(aggregate_id: Sourced.uuid, name: 'Joan', age: 38)
cmd.name # 'Joan'
cmd.age # 38
cmd.id # UUID
cmd.aggregate_idid # UUID
```

You can build events of the right class from a hash (uses `topic` to find class).

```ruby
# Will return a CreateUser command
cmd = Sourced::Event.from(
  topic: 'users.create',
  name: 'Joan',
  age: 38
)
```

### Aggregate

Aggregates are your domain objects. They encapsulate related properties and state, and guard against invalid state changes.
Aggregates are _not_ "models" in the ActiveRecord sense! They know nothing about databases.

Aggregates can hold whatever internal state you need, but they must be able to build said state entirely from events.
For a given list of events, an aggregate instance should arrive to the exact same state every time.

```ruby
class User
  include Sourced::Aggregate
  attr_reader :name, :age

  on UserCreated do |event|
    @id = event.aggregate_id
    @name = event.name
    @age = event.age
  end

  on UserNameUpdated do |event|
    @name = event.name
  end

  on UserAgeUpdated do |event|
    @age = event.age
  end
end
```

Applying events will move an aggregate's state forward.

```ruby
user = User.new
user.apply UserCreated, aggregated_id: Sourced.uuid, name: 'Joe', age: 30
user.name # 'Joe'
user.age # 30
user.version # 1, tracked automatically

user.apply UserAgeUpdated, age: 40
user.age # 40
user.version # 2

# Instance collects applied events
user.events # [<UserCreated>, <UserAgeUpdated>]
```

You can apply events directly to aggregates, but you can also add your own methods and guard against invalid state changes.

```ruby
class User
  include Sourced::Aggregate

  def start(id:, name:, age:)
    raise "already started" if self.id
    apply UserCreated, aggregate_id: id, name: name, age: age
  end

  def name=(n)
    raise "not created yet" unless id
    apply UserNameUpdated, name: n
  end

  def age=(a)
    raise "not created yet" unless id
    apply UserAgeUpdated, age: a
  end

  # event blocks here
  on UserCreated do |event|
    # etc
  end
end
```

Now the outside world can treat your users as regular Ruby objects.

```ruby
user = User.new
user.start(id: Sourced.uuid, name: 'Joe', age: 30)
user.age = 40
user.events # [<UserCreated>, <UserAgeUpdated>]
```

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
