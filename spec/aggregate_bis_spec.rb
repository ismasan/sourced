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

  class AggregateRoot
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

    def initialize(projector: self.class.projection, entity_constructor: self.class.entity)
      @projector = projector
      @entity_constructor = entity_constructor
    end

    def build(id)
      @entity_constructor.call(id)
    end

    def load(id, stream)
      entity = build(id)
      seq = 0
      stream.each do |evt|
        seq = evt.seq
        @projector.call(evt, entity)
      end

      AggregateSession.new(id, entity, @projector, seq: seq)
    end
  end

  class AggregateSession
    attr_reader :id, :entity, :events, :seq

    def initialize(id, entity, projector, seq: 0)
      @id = id
      @entity = entity
      @projector = projector
      @seq = seq
      @events = []
    end

    def ==(other)
      other.id == id && other.seq == seq
    end

    def inspect
      %(<#{self.class.name}##{id} #{events.size} uncommitted events #{entity} >)
    end

    def apply(event_or_class, attrs = {})
      attrs = attrs.dup
      event = if event_or_class.respond_to?(:new!)
        event_or_class.new!(next_event_attrs.merge(attrs))
      else
        event_or_class
      end
      projector.call(event, entity)
      @seq = event.seq
      events << event
      self
    end

    def clear_events
      @events.slice!(0, @events.size)
    end

    private

    attr_reader :projector

    def next_event_attrs
      {
        aggregate_id: id,
        seq: seq + 1,
      }
    end
  end

  class User < AggregateRoot
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
    e1 = UserDomain::UserCreated.new!(aggregate_id: id, name: 'Joe', age: 41, seq: 1)
    e2 = UserDomain::NameChanged.new!(aggregate_id: id, name: 'Ismael', seq: 2)
    e3 = UserDomain::AgeChanged.new!(aggregate_id: id, age: 42, seq: 3)
    stream = [e1, e2, e3]

    user = User.load(id, stream)
    expect(user.seq).to eq 3
    expect(user.entity[:id]).to eq id
    expect(user.entity[:name]).to eq 'Ismael'
    expect(user.entity[:age]).to eq 42

    user.apply(UserDomain::NameChanged, name: 'Ismael 2')
    user.apply(UserDomain::AgeChanged, age: 43)

    expect(user.id).to eq id
    expect(user.seq).to eq 5
    expect(user.entity[:name]).to eq 'Ismael 2'
    expect(user.entity[:age]).to eq 43
    expect(user.events.size).to eq 2
    user.clear_events.tap do |events|
      expect(events.map(&:class)).to eq([UserDomain::NameChanged, UserDomain::AgeChanged])
      expect(events.map(&:seq)).to eq([4, 5])
    end
    expect(user.events.size).to eq(0)
  end
end
