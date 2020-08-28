# frozen_string_literal: true

module UserDomain
  ## Commands
  CreateUser = Sourced::Event.define('users.create') do
    attribute :name, Sourced::Types::String
    attribute :age, Sourced::Types::Coercible::Integer
  end
  UpdateUser = Sourced::Event.define('users.update') do
    attribute :name, Sourced::Types::String
    attribute :age, Sourced::Types::Coercible::Integer
  end

  ## Events
  UserCreated = Sourced::Event.define('users.created') do
    attribute :name, Sourced::Types::String
    attribute :age, Sourced::Types::Coercible::Integer
  end
  NameChanged = Sourced::Event.define('users.name.changed') do
    attribute :name, Sourced::Types::String
  end
  AgeChanged = Sourced::Event.define('users.age.changed') do
    attribute :age, Sourced::Types::Coercible::Integer
  end

  class UserSession < Sourced::EntitySession
    User = Struct.new(:id, :name, :age)

    entity do |id|
      User.new(id, '', 0)
    end

    projector do
      on UserCreated do |evt, user|
        user.id = evt.entity_id
        user.name = evt.payload.name
        user.age = evt.payload.age
      end
      on NameChanged do |evt, user|
        user.name = evt.payload.name
      end
      on AgeChanged do |evt, user|
        user.age = evt.payload.age
      end
    end
  end
end
