# frozen_string_literal: true

require 'spec_helper'
require 'sourced/command_methods'

module CMDMethodsTest
  class Actor < Sourced::Actor
    include Sourced::CommandMethods

    UpdateAge = Sourced::Message.define('cmdtest.update_age') do
      attribute :age, Integer
    end

    command :start, name: String do |_, cmd|
      event :started, cmd.payload
    end

    event :started, name: String

    command UpdateAge do |_, cmd|
      event :age_updated, cmd.payload
    end

    event :age_updated, age: Integer
  end
end

RSpec.describe Sourced::CommandMethods do
  describe 'in-memory version (#start, #update_age)' do
    it 'creates method for symbolised commands' do
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      cmd, new_events = actor.start(name: 'Joe')
      expect(cmd.valid?).to be(true)
      expect(actor.seq).to eq(1)
      expect(new_events).to match_sourced_messages([
        CMDMethodsTest::Actor::Started.build('aa', name: 'Joe')
      ])
    end

    it 'creates method for command class' do
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      cmd, new_events = actor.update_age(age: 10)
      expect(cmd.valid?).to be(true)
      expect(new_events).to match_sourced_messages([
        CMDMethodsTest::Actor::AgeUpdated.build('aa', age: 10)
      ])
    end

    it 'returns invalid command on invalid arguments' do
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      cmd, new_events = actor.start(name: 20)
      expect(cmd.valid?).to be(false)
      expect(actor.seq).to eq(0)
      expect(new_events).to eq([])
    end
  end

  describe 'durable version that saves messages to backend (#start!, #update_age!)' do
    before do
      Sourced.config.backend.clear!
    end

    it 'appends messages to backend' do
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      cmd, new_events = actor.start!(name: 'Joe')
      expect(cmd.valid?).to be(true)
      expect(actor.seq).to eq(1)
      events = Sourced.config.backend.read_stream(actor.id)
      expect(events).to eq(new_events)
      expect(new_events).to match_sourced_messages([
        CMDMethodsTest::Actor::Started.build('aa', name: 'Joe')
      ])
    end

    it 'works when command is a class' do
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      cmd, new_events = actor.update_age!(age: 20)
      expect(cmd.valid?).to be(true)
      expect(actor.seq).to eq(1)
      events = Sourced.config.backend.read_stream(actor.id)
      expect(events).to eq(new_events)
      expect(new_events).to match_sourced_messages([
        CMDMethodsTest::Actor::AgeUpdated.build('aa', age: 20)
      ])
    end

    it 'validates command' do
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      cmd, new_events = actor.update_age!(age: 'nope')
      expect(cmd.valid?).to be(false)
      expect(actor.seq).to eq(0)
      expect(new_events).to eq([])
    end

    it 'raises an exception if backend fails to append' do
      allow(Sourced.config.backend).to receive(:append_to_stream).and_return(false)
      actor = CMDMethodsTest::Actor.new(id: 'aa')
      expect {
        actor.update_age!(age: 20)
      }.to raise_error(Sourced::CommandMethods::FailedToAppendMessagesError)
    end
  end
end
