require 'spec_helper'

RSpec.describe Sourced::Dispatcher do
  let(:store) { Sourced::MemEventStore.new }
  let(:repo) { Sourced::AggregateRepo.new(event_store: store) }
  let(:subscribers) { Sourced::Subscribers.new }
  subject(:dispatcher) {
    described_class.new(
      repository: repo,
      handler: UserDomain::UserHandler,
      subscribers: subscribers
    )
  }

  it 'passes commands to handler and returns updated aggregate' do
    id = Sourced.uuid

    create = UserDomain::CreateUser.new!(entity_id: id, name: 'Ismael', age: 40)
    user = dispatcher.call(create)

    expect(user.id).to eq create.entity_id
    expect(user.name).to eq 'Ismael'
    expect(user.age).to eq 40
    expect(user.seq).to eq 3

    update = UserDomain::UpdateUser.new!(entity_id: id, age: 41)
    user_b = dispatcher.call(update)

    expect(user_b.id).to eq user.id
    expect(user_b.name).to eq 'Ismael'
    expect(user_b.age).to eq 41
    expect(user_b.seq).to eq 4
  end

  it 'appends resulting events to event store, including commands' do
    id = Sourced.uuid

    create = UserDomain::CreateUser.new!(entity_id: id, name: 'Ismael', age: 40)
    dispatcher.call(create)
    update = UserDomain::UpdateUser.new!(entity_id: id, age: 41)
    dispatcher.call(update)

    events = store.by_entity_id(id)

    expect(events.map(&:topic)).to eq %w(users.create users.created users.name.changed users.age.changed users.update users.age.changed)
    # commands don't increment seq
    expect(events.map(&:seq)).to eq [1, 1, 2, 3, 1, 4]
    expect(events.map(&:parent_id).uniq.compact).to eq [create.id, update.id]
  end

  it 'sends events to subscribers' do
    id = Sourced.uuid
    create = UserDomain::CreateUser.new!(entity_id: id, name: 'Ismael', age: 40)

    expect(subscribers).to receive(:call) do |events|
      expect(events.map(&:topic)).to eq %w(users.create users.created users.name.changed users.age.changed)
    end

    dispatcher.call(create)
  end

  it 'raises a useful error when no handlers found for a given command' do
    cmd_class = Sourced::Event.define('foo.bar')
    expect {
      dispatcher.call(cmd_class.new!(entity_id: Sourced.uuid))
    }.to raise_error(Sourced::UnhandledCommandError)
  end

  it 'can take a run-time handler' do
    cmd = double('Command', topic: 'foobar', entity_id: Sourced.uuid)
    aggregate = double('Aggregate', clear_events: [])
    handler = double('Handler',
                     aggregate_class: UserDomain::User,
                     call: aggregate,
                     topics: ['foobar']
                    )
    aggr = dispatcher.call(cmd, handler: handler)

    expect(aggr).to eq(aggregate)
  end
end
