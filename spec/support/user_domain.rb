module UserDomain
  ## Commands
  CreateUser = Sourced::Event.define('users.create') do
    field(:name).type(:string).present
    field(:age).type(:integer).present
  end
  UpdateUser = Sourced::Event.define('users.update') do
    field(:name).type(:string)
    field(:age).type(:integer)
  end

  ## Events
  UserCreated = Sourced::Event.define('users.created') do
    field(:name).type(:string).present
    field(:age).type(:integer).present
  end
  NameChanged = Sourced::Event.define('users.name.changed') do
    field(:name).type(:string).present
  end
  AgeChanged = Sourced::Event.define('users.age.changed') do
    field(:age).type(:integer).present
  end

  ## Aggregate
  class User < Sourced::Aggregate
    attr_reader :name, :age

    def start(name, age)
      apply UserCreated, payload: { name: name, age: age }
    end

    def name=(n)
      apply NameChanged, payload: { name: n }
    end

    def age=(a)
      apply AgeChanged, payload: { age: a }
    end

    on UserCreated do |evt|
      @id = evt.entity_id
      @name = evt.payload.name
      @age = evt.payload.age
    end
    on NameChanged do |evt|
      @name = evt.payload.name
    end
    on AgeChanged do |evt|
      @age = evt.payload.age
    end
  end

  ## Handler
  class UserHandler
    include Sourced::CommandHandler

    aggregates UserDomain::User

    on UserDomain::CreateUser do |cmd, user|
      user.apply UserDomain::UserCreated, entity_id: cmd.entity_id, payload: { name: 'foo', age: 10 }
      user.apply UserDomain::NameChanged, payload: { name: cmd.payload.name }
      user.apply UserDomain::AgeChanged, payload: { age: cmd.payload.age }
    end

    on UserDomain::UpdateUser do |cmd, user|
      user.apply(UserDomain::NameChanged, payload: { name: cmd.payload.name }) if cmd.payload.name
      user.apply(UserDomain::AgeChanged, payload: { age: cmd.payload.age }) if cmd.payload.age
    end
  end
end
