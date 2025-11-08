# frozen_string_literal: true

require 'spec_helper'
require 'sourced/testing/rspec'

module Testing
  Start = Sourced::Message.define('sourced.testing.start') do
    attribute :name, String
  end

  Started = Sourced::Message.define('sourced.testing.started') do
    attribute :name, String
  end

  class Actor < Sourced::Actor
    state do |id|
      { id:, name: nil}
    end

    command Start do |state, cmd|
      if state[:name].nil?
        event Started, cmd.payload
      end
    end

    event Started do |state, evt|
      state[:name] = evt.payload.name
    end
  end
end

RSpec.describe Sourced::Testing::RSpec do
  include described_class

  context 'with Actor instance' do
    it 'works' do
      with_actor(Testing::Actor.new(id: 'a'))
        .when(Testing::Start, name: 'Joe')
        .then(Testing::Started.build('a', name: 'Joe'))

      with_actor(Testing::Actor.new(id: 'a'))
        .when(Testing::Start, name: 'Joe')
        .then(Testing::Started, name: 'Joe')

      with_actor(Testing::Actor.new(id: 'a'))
        .when(Testing::Start, name: 'Joe')
        .then([Testing::Started.build('a', name: 'Joe')])

      with_actor(Testing::Actor.new(id: 'a'))
        .given(Testing::Started, name: 'Joe')
        .when(Testing::Start, name: 'Joe')
        .then([])
    end
  end
end
