# frozen_string_literal: true

require 'spec_helper'

module TestSetup
  TodoList = Struct.new(:seq, :id, :items, :notified)

  AddItem = Sourced::Message.define('imm.todos.add') do
    attribute :name, String
  end

  Notify = Sourced::Message.define('imm.todos.notify')
  Notified = Sourced::Message.define('imm.todos.notified')

  ItemAdded = Sourced::Message.define('imm.todos.added') do
    attribute :name, String
  end

  class TodoListDecider < Sourced::Decider
    consumer do |c|
      c.sync!
    end

    def init_state(id)
      TodoList.new(0, id, [], false)
    end

    decide AddItem do |_list, cmd|
      apply ItemAdded, name: cmd.payload.name
    end

    evolve ItemAdded do |list, event|
      list.items << event.payload
    end

    react ItemAdded do |event|
      event.follow(Notify)
    end

    decide Notify do |_list, _cmd|
      apply Notified
    end

    evolve Notified do |list, _event|
      list.notified = true
    end
  end
end

RSpec.describe 'Immediate consistency' do
  before do
    Sourced.config.backend.clear!
    Sourced::Router.register(TestSetup::TodoListDecider)
  end

  let(:cmd) { TestSetup::AddItem.parse(stream_id: 'list1', payload: { name: 'item1' }) }

  it 'runs reactor immediately' do
    decider, _events = TestSetup::TodoListDecider.handle_command(cmd)
    decider.catch_up
    expect(decider.state.notified).to be(true)
    expect(TestSetup::TodoListDecider.load(decider.id).state.notified).to be(true)
  end
end
