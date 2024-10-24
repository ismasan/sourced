# frozen_string_literal: true

require 'spec_helper'
require_relative './support/test_aggregate'

RSpec.describe Sors::Aggregate do
  specify 'invalid commands' do
    list = TestAggregate::TodoList.create
    cmd = list.add(name: 10)
    expect(cmd.valid?).to be(false)
    expect(list.items.size).to eq(0)
  end

  specify 'handling commands' do
    list = TestAggregate::TodoList.create
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
    list = TestAggregate::TodoList.create
    list.add(name: 'Buy milk')
    list.add(name: 'Buy bread')

    list.mark_done(list.items.first.id)
    list.mark_done(list.items.last.id)

    Sors::Worker.drain

    list = TestAggregate::TodoList.load(list.id)
    expect(list.email_sent).to be(true)
  end
end
