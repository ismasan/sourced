# frozen_string_literal: true

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
