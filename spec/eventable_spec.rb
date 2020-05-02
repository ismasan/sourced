# frozen_string_literal: true

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
      attr_reader :id, :name, :seq

      on ETE::UserCreated do |event|
        @id = event.entity_id
      end

      # multiple handlers don't break anything
      on ETE::UserCreated do |event|

      end

      on 'users.name_changed' do |event|
        @name = event.payload.name
      end

      on :any do |event|
        @seq = event.seq
      end
    end
  }

  it '.topics' do
    expect(user_class.topics).to eq %w(users.created users.name_changed)
  end

  it '#topics' do
    expect(user_class.new.topics).to eq %w(users.created users.name_changed)
  end

  describe '#apply' do
    it 'updates state' do
      id = Sourced.uuid
      user = user_class.new
      user.apply ETE::UserCreated, entity_id: id, seq: 1
      user.apply ETE::NameChanged, entity_id: id, seq: 2, payload: { name: 'Ismael' }
      expect(user.id).to eq id
      expect(user.name).to eq 'Ismael'
      expect(user.seq).to eq 2
    end
  end

  it 'inherits handlers' do
    sub_class = Class.new(user_class) do
      attr_reader :title, :age

      # register a second handler for same event
      on ETE::NameChanged do |event|
        @title = "Mr. #{event.payload.name}"
      end

      on ETE::AgeChanged do |event|
        @age = event.payload.age
      end
    end

    id = Sourced.uuid
    user = sub_class.new
    user.apply ETE::UserCreated, entity_id: id
    user.apply ETE::NameChanged, entity_id: id, payload: { name: 'Ismael' }
    user.apply ETE::AgeChanged, entity_id: id, payload: { age: 41 }

    expect(user.name).to eq 'Ismael'
    expect(user.title).to eq 'Mr. Ismael'
    expect(user.age).to eq 41
  end
end

