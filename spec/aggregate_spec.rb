# frozen_string_literal: true

require 'spec_helper'
require_relative './support/test_aggregate'

RSpec.describe Sors::Aggregate do
  specify 'handling commands' do
    list = TestAggregate::TodoList.create
    list.add(name: 'Buy milk')
    list.add(name: 'Buy bread')
    list.mark_done(list.items.first.id)

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
