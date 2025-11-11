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

  class Reactor
    extend Sourced::Consumer

    def self.handled_messages = [Start]

    def self.handle(message, history: [])
      actions = []
      if Start === message && history.none? { |m| Started === m }
        actions << Sourced::Actions::AppendNext.new([message.follow(Started, name: message.payload.name)])
      end
      actions
    end
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

  context 'with Reactor interface' do
    it 'works' do
      with_reactor(Testing::Reactor, 'a')
        .when(Testing::Start, name: 'Joe')
        .then(Testing::Started.build('a', name: 'Joe'))

      with_reactor(Testing::Reactor, 'a')
        .given(Testing::Started, name: 'Joe')
        .when(Testing::Start, name: 'Joe')
        .then([])

      # If supports any .handle() interface, including Actor classes
      with_reactor(Testing::Actor, 'a')
        .when(Testing::Start, name: 'Joe')
        .then(Testing::Started.build('a', name: 'Joe'))
    end
  end

  context 'with Actor instance' do
    it 'works' do
      with_reactor(Testing::Actor.new(id: 'a'))
        .when(Testing::Start, name: 'Joe')
        .then(Testing::Started.build('a', name: 'Joe'))

      with_reactor(Testing::Actor.new(id: 'a'))
        .when(Testing::Start, name: 'Joe')
        .then(Testing::Started, name: 'Joe')

      with_reactor(Testing::Actor.new(id: 'a'))
        .when(Testing::Start, name: 'Joe')
        .then([Testing::Started.build('a', name: 'Joe')])

      with_reactor(Testing::Actor.new(id: 'a'))
        .given(Testing::Started, name: 'Joe')
        .when(Testing::Start, name: 'Joe')
        .then([])
    end
  end

  specify 'it raises when adding events after assertion' do
    expect {
      with_reactor(Testing::Reactor, 'a')
        .given(Testing::Started, name: 'Joe')
        .when(Testing::Start, name: 'Joe')
        .then([])
        .given(Testing::Started, name: 'Joe') # <= can't add more state after .then() assertion
    }.to raise_error(Sourced::Testing::RSpec::FinishedTestCase)
  end

  context 'with block given to #then' do
    it 'evaluates block' do
      received = []

      klass = Class.new do
        extend Sourced::Consumer
        def self.handled_messages = [Testing::Start]
      end
      klass.define_singleton_method(:handle) do |message, history:|
        received << message
        []
      end

      with_reactor(klass, 'abc')
        .when(Testing::Start, name: 'Joe')
        .then do |actions|
          expect(actions).to eq([])
          expect(received).to match_sourced_messages(Testing::Start.build('abc', name: 'Joe'))
        end
    end
  end

  describe '.then!' do
    it 'evaluates sync blocks' do
      received = []

      klass = Class.new do
        extend Sourced::Consumer
        def self.handled_messages = [Testing::Start]
      end
      klass.define_singleton_method(:handle) do |message, history:|
        sync = proc do
          received << 10
        end
        started = message.follow(Testing::Started, message.payload)
        [
          Sourced::Actions::Sync.new(sync), 
          Sourced::Actions::AppendNext.new([started])
        ]
      end

      with_reactor(klass, 'abc')
        .when(Testing::Start, name: 'Joe')
        .then! do |actions|
          expect(actions.first).to be_a(Sourced::Actions::Sync)
          expect(received).to eq([10])
        end
        .then(Testing::Started.build('abc', name: 'Joe'))
    end
  end
end
