require 'spec_helper'

RSpec.describe Sourced::Event do
  let(:user_created) {
    Sourced::Event.define('users.created') do
      field(:name).type(:string).present
      field(:age).type(:integer).default(40)
    end
  }

  context 'class-level' do
    it '.topic' do
      expect(user_created.topic).to eq 'users.created'
    end

    describe '.instance' do
      it 'instantiates with valid payload' do
        evt = user_created.instance(
          aggregate_id: Sourced.uuid,
          name: 'Ismael'
        )

        expect(evt.topic).to eq 'users.created'
        expect(evt.name).to eq 'Ismael'
        expect(evt.id).not_to be nil
        expect(evt.date).not_to be nil
        expect(evt.age).to eq 40 # default
      end

      it 'raises with invalid payload' do
        expect {
          user_created.instance(
            aggregate_id: Sourced.uuid,
          )
        }.to raise_error Sourced::InvalidEventError
      end
    end

    describe '.from' do
      it 'finds subclass from topic and builds event' do
        id = Sourced.uuid
        aggrid = Sourced.uuid
        data = {
          id: id,
          aggregate_id: aggrid,
          topic: 'users.name.changed',
          name: 'Joe',
        }

        evt = Sourced::Event.from(data)
        expect(evt).to be_a UserDomain::NameChanged
        expect(evt.id).to eq id
        expect(evt.aggregate_id).to eq aggrid
        expect(evt.name).to eq 'Joe'
      end
    end
  end

  context 'instance-level' do
    describe '#copy' do
      it 'produces copy of the same class, with optional new attributes' do
        aggrid = Sourced.uuid
        parent_id = Sourced.uuid
        evt1 = user_created.instance(aggregate_id: aggrid, name: 'Ismael', age: 40)
        evt2 = evt1.copy(parent_id: parent_id)

        expect(evt1.id).to eq evt2.id
        expect(evt1.aggregate_id).to eq aggrid
        expect(evt1.aggregate_id).to eq evt2.aggregate_id
        expect(evt1.name).to eq evt2.name
        expect(evt1.parent_id).to be nil
        expect(evt2.parent_id).to eq parent_id
      end
    end
  end
end
