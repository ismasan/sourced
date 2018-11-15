require 'spec_helper'

RSpec.describe Sourced::Dispatcher do
  let(:store) { Sourced::MemEventStore.new }
  let(:repo) { Sourced::AggregateRepo.new(event_store: store) }
  let(:subscribers) { Sourced::Subscribers.new }
  subject(:dispatcher) {
    described_class.new(
      repository: repo,
      store: store,
      handler: UserDomain::UserHandler,
      subscribers: subscribers
    )
  }

  it 'passes commands to handler and returns updated aggregate' do
    id = Sourced.uuid

    create = UserDomain::CreateUser.new!(aggregate_id: id, name: 'Ismael', age: 40)
    user = dispatcher.call(create)

    expect(user.id).to eq create.aggregate_id
    expect(user.name).to eq 'Ismael'
    expect(user.age).to eq 40
    expect(user.version).to eq 3

    update = UserDomain::UpdateUser.new!(aggregate_id: id, age: 41)
    user_b = dispatcher.call(update)

    expect(user_b.id).to eq user.id
    expect(user_b.name).to eq 'Ismael'
    expect(user_b.age).to eq 41
    expect(user_b.version).to eq 4
  end

  it 'appends resulting events to event store, including commands' do
    id = Sourced.uuid

    create = UserDomain::CreateUser.new!(aggregate_id: id, name: 'Ismael', age: 40)
    dispatcher.call(create)
    update = UserDomain::UpdateUser.new!(aggregate_id: id, age: 41)
    dispatcher.call(update)

    events = store.by_aggregate_id(id)

    expect(events.map(&:topic)).to eq %w(users.create users.created users.name.changed users.age.changed users.update users.age.changed)
    # commands don't increment version
    expect(events.map(&:version)).to eq [1, 1, 2, 3, 1, 4]
    expect(events.map(&:parent_id).uniq.compact).to eq [create.id, update.id]
  end

  it 'sends events to subscribers' do
    id = Sourced.uuid
    create = UserDomain::CreateUser.new!(aggregate_id: id, name: 'Ismael', age: 40)

    expect(subscribers).to receive(:call) do |events|
      expect(events.map(&:topic)).to eq %w(users.create users.created users.name.changed users.age.changed)
    end

    dispatcher.call(create)
  end

  it 'raises a useful error when no handlers found for a given command' do
    cmd_class = Sourced::Event.define('foo.bar')
    expect {
      dispatcher.call(cmd_class.new!(aggregate_id: Sourced.uuid))
    }.to raise_error(Sourced::UnhandledCommandError)
  end
end
