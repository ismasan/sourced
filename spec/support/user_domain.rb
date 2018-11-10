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
  User = Class.new do
    include Sourced::Aggregate
    attr_reader :name, :age
    def initialize(id)
      @id = id
      @name = nil
      @age = nil
    end

    def start(name, age)
      apply UserCreated, name: name, age: age
    end

    def name=(n)
      apply NameChanged, name: n
    end

    def age=(a)
      apply AgeChanged, age: a
    end

    on UserCreated do |evt|
      @id = evt.aggregate_id
      @name = evt.name
      @age = evt.age
    end
    on NameChanged do |evt|
      @name = evt.name
    end
    on AgeChanged do |evt|
      @age = evt.age
    end
  end

  ## Handler
  class UserHandler < Sourced::CommandHandler
    aggregates UserDomain::User

    on UserDomain::CreateUser do |cmd, user|
      user.apply UserDomain::UserCreated, aggregate_id: cmd.aggregate_id, name: 'foo', age: 10
      user.apply UserDomain::NameChanged, name: cmd.name
      user.apply UserDomain::AgeChanged, age: cmd.age
    end

    on UserDomain::UpdateUser do |cmd, user|
      user.apply(UserDomain::NameChanged, name: cmd.name) if cmd.name
      user.apply(UserDomain::AgeChanged, age: cmd.age) if cmd.age
    end
  end
end
