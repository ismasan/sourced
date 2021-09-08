# frozen_string_literal: true

module Sourced
  RSpec.shared_examples 'a valid Sourced event' do
    let(:event) { event_constructor.new(attributes) }

    specify '.topic' do
      expect(event_constructor.topic).to be_a(String)
    end

    specify '#id' do
      expect(event.id).not_to be_nil
    end

    specify '#entity_id' do
      expect(event.entity_id).not_to be_nil
    end

    specify '#date' do
      expect(event.date).not_to be_nil
    end

    specify '#to_h' do
      hash = event.to_h
      expect(hash[:topic]).to eq(event_constructor.topic)
      attributes.each do |k, v|
        expect(hash[k]).to eq(v)
      end
    end

    specify '#copy' do
      new_entity_id = Sourced.uuid
      event2 = event.copy(entity_id: new_entity_id)
      expect(event2).to be_a(event.class)
      expect(event2.entity_id).to eq(new_entity_id)
      expect(event2.id).to eq(event.id)
    end
  end
end
