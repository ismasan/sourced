# frozen_string_literal: true

require 'spec_helper'

class SyncTodoListActor < Sourced::Actor
  TodoList = Struct.new(:seq, :id, :items, :notified)

  consumer do |c|
    c.sync!
  end

  state do |id|
    TodoList.new(0, id, [], false)
  end

  command :add_item, name: String do |_list, cmd|
    event :item_added, name: cmd.payload.name
  end

  event :item_added, name: String do |list, event|
    list.items << event.payload
  end

  react :item_added do |_event|
    command :notify
  end

  command :notify do |_list, _cmd|
    event :notified
  end

  event :notified do |list, _event|
    list.notified = true
  end
end

RSpec.describe 'Immediate consistency' do
  before do
    Sourced.config.backend.clear!
    Sourced.register(SyncTodoListActor)
  end

  let(:cmd) { SyncTodoListActor[:add_item].parse(stream_id: 'list1', payload: { name: 'item1' }) }

  it 'runs reactor immediately' do
    decider, _events = SyncTodoListActor.handle_command(cmd)
    decider.catch_up
    expect(decider.state.notified).to be(true)
    expect(SyncTodoListActor.load(decider.id).state.notified).to be(true)
  end
end
