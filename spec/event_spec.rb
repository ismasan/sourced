require 'spec_helper'
require 'sourced/event'

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
  end
end
