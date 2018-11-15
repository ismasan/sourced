require 'spec_helper'

RSpec.describe Sourced::Subscribers do
  let(:topic_counter_projection) {
    Class.new do
      include Sourced::Eventable

      attr_reader :counts
      def initialize
        @counts = Hash.new(0)
      end

      on UserDomain::UserCreated do |evt|
        @counts[evt.topic] += 1
      end

      on UserDomain::NameChanged do |evt|
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

      on UserDomain::UserCreated do |evt|
        @counts.add evt.aggregate_id
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

    subs.call(UserDomain::UserCreated.new!(aggregate_id: id1, name: 'Ismael', age: 40))
    subs.call(UserDomain::NameChanged.new!(aggregate_id: id1, name: 'Joe'))
    subs.call(UserDomain::UserCreated.new!(aggregate_id: id2, name: 'Joan', age: 41))

    expect(topic_counter.counts['users.created']).to eq 2
    expect(topic_counter.counts['users.name.changed']).to eq 1

    expect(user_counter.total).to eq 2
  end
end
