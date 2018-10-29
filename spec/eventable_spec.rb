require 'spec_helper'

RSpec.describe Sourced::Eventable do
  module ETE
    UserCreated = Sourced::Event.define('users.created')
    NameChanged = Sourced::Event.define('users.name_changed') do
      field(:name).type(:string)
    end
    AgeChanged = Sourced::Event.define('users.age_changed') do
      field(:age).type(:integer)
    end
  end

  let(:user_class) {
    Class.new do
      include Sourced::Eventable
      attr_reader :id, :name

      def initialize(id = nil)
        apply ETE::UserCreated.instance(aggregate_id: id) if id
      end

      def name=(n)
        apply ETE::NameChanged.instance(aggregate_id: id, name: n)
      end

      on ETE::UserCreated do |event|
        @id = event.aggregate_id
      end

      on 'users.name_changed' do |event|
        @name = event.name
      end
    end
  }

  it 'works' do
    id = Sourced.uuid
    user = user_class.new(id)
    user.name = 'Ismael'
    expect(user.id).to eq id
    expect(user.name).to eq 'Ismael'
    topics = %w(users.created users.name_changed)
    expect(user.events.map(&:topic)).to eq topics
    expect(user.clear_events.map(&:topic)).to eq topics
    expect(user.events.size).to eq 0
  end

  it 'inherits handlers' do
    sub_class = Class.new(user_class) do
      attr_reader :title, :age

      def age=(int)
        apply ETE::AgeChanged.instance(age: int)
      end

      # register a second handler for same event
      on ETE::NameChanged do |event|
        @title = "Mr. #{event.name}"
      end

      on ETE::AgeChanged do |event|
        @age = event.age
      end
    end

    user = sub_class.new(Sourced.uuid)
    user.name = 'Ismael'
    user.age = 30

    expect(user.title).to eq 'Mr. Ismael'
    expect(user.age).to eq 30
  end
end

