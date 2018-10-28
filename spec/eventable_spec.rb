require 'spec_helper'

RSpec.describe Sourced::Eventable do
  UserCreated = Sourced::Event.define('users.created')
  NameChanged = Sourced::Event.define('users.name_changed') do
    field(:name).type(:string)
  end
  AgeChanged = Sourced::Event.define('users.age_changed') do
    field(:age).type(:integer)
  end

  let(:user_class) {
    Class.new do
      include Sourced::Eventable
      attr_reader :id, :name

      def initialize(id = nil)
        apply UserCreated.instance(aggregate_id: id) if id
      end

      def name=(n)
        apply NameChanged.instance(aggregate_id: id, name: n)
      end

      on UserCreated do |event|
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
    expect(user.events.map{|e| e.class.to_s }).to eq ['UserCreated', 'NameChanged']
  end
end

