# frozen_string_literal: true

require 'spec_helper'
require 'json'

RSpec.describe Sourced::Event do
  let(:user_created) {
    Sourced::Event.define('users.created') do
      attribute :name, Sourced::Types::String
      attribute :age, Sourced::Types::Coercible::Integer.default(40)
    end
  }

  context 'class-level' do
    it '.topic' do
      expect(user_created.topic).to eq 'users.created'
    end

    describe '.new!' do
      it 'instantiates with valid payload' do
        evt = user_created.new!(
          entity_id: Sourced.uuid,
          payload: {
            name: 'Ismael'
          }
        )

        expect(evt.topic).to eq 'users.created'
        expect(evt.payload.name).to eq 'Ismael'
        expect(evt.id).not_to be nil
        expect(evt.date).not_to be nil
        expect(evt.payload.age).to eq 40 # default
      end

      it 'raises with invalid payload' do
        expect {
          user_created.new!(
            entity_id: Sourced.uuid,
          )
        }.to raise_error Dry::Struct::Error
      end
    end

    describe '.from' do
      it 'finds subclass from topic and builds event' do
        id = Sourced.uuid
        aggrid = Sourced.uuid
        data = {
          id: id,
          entity_id: aggrid,
          topic: 'users.name.changed',
          payload: {
            name: 'Joe'
          }
        }

        evt = Sourced::Event.from(data)
        expect(evt).to be_a UserDomain::NameChanged
        expect(evt.id).to eq id
        expect(evt.entity_id).to eq aggrid
        expect(evt.payload.name).to eq 'Joe'
      end
    end
  end

  context 'instance-level' do
    describe '#copy' do
      it 'produces copy of the same class, with optional new attributes' do
        aggrid = Sourced.uuid
        originator_id = Sourced.uuid
        evt1 = user_created.new!(entity_id: aggrid, payload: { name: 'Ismael', age: 40 })
        evt2 = evt1.copy(originator_id: originator_id)

        expect(evt1.id).to eq evt2.id
        expect(evt1.entity_id).to eq aggrid
        expect(evt1.entity_id).to eq evt2.entity_id
        expect(evt1.payload.name).to eq evt2.payload.name
        expect(evt1.originator_id).to be nil
        expect(evt2.originator_id).to eq originator_id
      end
    end
  end

  context 'when loading from JSON' do
    it 'preserves equality' do
      uuid = Sourced.uuid
      e1 = user_created.new!(entity_id: uuid, payload: { name: 'Ismael', age: 40 })
      json = JSON.dump(e1.to_h)
      data = JSON.parse(json, symbolize_names: true)
      e2 = user_created.new!(data)
      expect(e1).to eq(e2)
    end
  end
end
