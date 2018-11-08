require 'spec_helper'

RSpec.describe Sourced::Dispatcher do
  let(:store) { Sourced::MemEventStore.new }
  let(:repo) { Sourced::AggregateRepo.new(event_store: store) }
  subject(:dispatcher) {
    described_class.new(
      repository: repo,
      store: store,
      handler: UserDomain::UserHandler
    )
  }

  it 'passes commands to handler and returns updated aggregate' do
    id = Sourced.uuid

    create = UserDomain::CreateUser.instance(aggregate_id: id, name: 'Ismael', age: 40)
    user = dispatcher.call(create)

    expect(user.id).to eq create.aggregate_id
    expect(user.name).to eq 'Ismael'
    expect(user.age).to eq 40
    expect(user.version).to eq 3

    update = UserDomain::UpdateUser.instance(aggregate_id: id, age: 41)
    user_b = dispatcher.call(update)

    expect(user_b.id).to eq user.id
    expect(user_b.name).to eq 'Ismael'
    expect(user_b.age).to eq 41
    expect(user_b.version).to eq 4
  end

  it 'appends resulting events to event store, including commands' do
    id = Sourced.uuid

    create = UserDomain::CreateUser.instance(aggregate_id: id, name: 'Ismael', age: 40)
    dispatcher.call(create)
    update = UserDomain::UpdateUser.instance(aggregate_id: id, age: 41)
    dispatcher.call(update)

    events = store.by_aggregate_id(id)

    expect(events.map(&:topic)).to eq %w(users.create users.created users.name.changed users.age.changed users.update users.age.changed)
    # commands don't increment version
    expect(events.map(&:version)).to eq [1, 1, 2, 3, 1, 4]
    expect(events.map(&:parent_id).uniq.compact).to eq [create.id, update.id]
  end
end