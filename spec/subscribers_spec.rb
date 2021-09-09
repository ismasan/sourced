# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sourced::Subscribers do
  let(:topic_counter_projection) {
    Class.new do
      include Sourced::Eventable

      attr_reader :counts
      def initialize
        @counts = Hash.new(0)
      end

      on Sourced::UserDomain::UserCreated do |evt|
        @counts[evt.topic] += 1
      end

      on Sourced::UserDomain::NameChanged do |evt|
        @counts[evt.topic] += 1
      end
    end
  }

  let(:user_counter_projection) {
    Class.new do
      include Sourced::Eventable

      def initialize
        @counts = Set.new
      end

      def total
        @counts.size
      end

      on Sourced::UserDomain::UserCreated do |evt|
        @counts.add evt.stream_id
      end
    end

  }

  it 'works' do
    topic_counter = topic_counter_projection.new
    user_counter = user_counter_projection.new

    subs = described_class.new
    subs.subscribe(topic_counter)
    subs.subscribe(user_counter)

    id1 = Sourced.uuid
    id2 = Sourced.uuid

    subs.call(Sourced::UserDomain::UserCreated.new(stream_id: id1, payload: { name: 'Ismael', age: 40 }))
    subs.call(Sourced::UserDomain::NameChanged.new(stream_id: id1, payload: { name: 'Joe' }))
    subs.call(Sourced::UserDomain::UserCreated.new(stream_id: id2, payload: { name: 'Joan', age: 41 }))

    expect(topic_counter.counts['users.created']).to eq 2
    expect(topic_counter.counts['users.name.changed']).to eq 1

    expect(user_counter.total).to eq 2
  end
end
