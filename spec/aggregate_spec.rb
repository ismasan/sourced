# frozen_string_literal: true

require 'spec_helper'
require_relative './support/test_aggregate'

RSpec.describe Sors::Aggregate do
  specify 'invalid commands' do
    list = TestAggregate::TodoList.build
    cmd = list.add(name: 10)
    expect(cmd.valid?).to be(false)
    expect(list.items.size).to eq(0)
  end

  specify 'handling commands' do
    list = TestAggregate::TodoList.build
    cmd = list.add(name: 'Buy milk')
    expect(cmd.valid?).to be(true)

    list.add(name: 'Buy bread')
    cmd = list.mark_done(list.items.first.id)
    expect(cmd.valid?).to be(true)

    expect(list.items.size).to eq(2)
    expect(list.items.filter(&:done).size).to eq(1)

    list2 = TestAggregate::TodoList.load(list.id)
    expect(list2.items.size).to eq(2)
    expect(list).to eq(list2)
  end

  specify 'reacting to events' do
    list = TestAggregate::TodoList.build
    list.add(name: 'Buy milk')
    list.add(name: 'Buy bread')

    list.mark_done(list.items.first.id)
    list.mark_done(list.items.last.id)
    expect(list.email_sent).to be(false)

    Sors::Worker.drain

    list.catch_up
    expect(list.email_sent).to be(true)
  end

  specify 'time travelling with .load(upto:), #catch_up and #events' do
    list = TestAggregate::TodoList.build
    list.add(name: 'milk')
    list.add(name: 'bread')
    list.add(name: 'apples')
    expect(list.items.map(&:name)).to eq(%w[milk bread apples])
    expect(list.seq).to eq(6)

    older = TestAggregate::TodoList.load(list.id, upto: 4)
    expect(older.items.map(&:name)).to eq(%w[milk bread])
    expect(older.seq).to eq(4)
    expect(older.events.map(&:seq)).to eq([1, 2, 3, 4])
    expect(older.events.map(&:type)).to eq(%w[
                                             todos.items.add
                                             todos.items.added
                                             todos.items.add
                                             todos.items.added
                                           ])

    older.catch_up
    expect(older.items.map(&:name)).to eq(%w[milk bread apples])
    expect(older.seq).to eq(6)
    expect(older.events.map(&:seq)).to eq([1, 2, 3, 4, 5, 6])
  end
end
