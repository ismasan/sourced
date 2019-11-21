require 'spec_helper'

RSpec.describe 'Composite aggregates' do
  class Projector
    include Sourced::Eventable

    def self.call(evt, entity)
      new.call(evt, entity)
    end

    def initialize(&block)
      instance_eval(&block) if block_given?
    end

    def call(evt, entity)
      apply(evt, deps: [entity], collect: false)
    end
  end

  class UserProjector < Projector
    on UserDomain::UserCreated do |event, user|
      user[:name] = event.name
      user[:age] = event.age
    end
    on UserDomain::AgeChanged do |event, user|
      user[:age] = event.age
    end
    on UserDomain::NameChanged do |event, user|
      user[:name] = event.name
    end
  end

  class Aggr
    def self.projection(pr = nil, &block)
      if pr
        @projector = pr
      elsif block_given?
        @projector = Class.new(Projector, &block)
      end

      @projector
    end

    def self.entity(entity = nil, &block)
      if entity
        @entity = ->(id) { entity }
      elsif block_given?
        @entity = block
      end

      @entity
    end

    def self.load(id, stream)
      new.load(id, stream)
    end

    def initialize(projector: self.class.projection, entity: self.class.entity)
      @projector = projector
      @entity = entity
    end

    def build(id)
      @entity.call(id)
    end

    def load(id, stream)
      instance = build(id)
      stream.each do |evt|
        @projector.call(evt, instance)
      end

      instance
    end
  end

  class User < Aggr
    entity do |id|
      {
        id: id,
        name: '',
        age: 0
      }
    end

    # projection UserProjector
    projection do
      on UserDomain::UserCreated do |event, user|
        user[:name] = event.name
        user[:age] = event.age
      end
      on UserDomain::AgeChanged do |event, user|
        user[:age] = event.age
      end
      on UserDomain::NameChanged do |event, user|
        user[:name] = event.name
      end
    end
  end

  it 'works' do
    id = Sourced.uuid
    e1 = UserDomain::UserCreated.new!(aggregate_id: id, name: 'Joe', age: 41)
    e2 = UserDomain::NameChanged.new!(aggregate_id: id, name: 'Ismael')
    e3 = UserDomain::AgeChanged.new!(aggregate_id: id, age: 42)
    stream = [e1, e2, e3]

    user = User.load(id, stream)
    expect(user[:id]).to eq id
    expect(user[:name]).to eq 'Ismael'
    expect(user[:age]).to eq 42
  end
end
