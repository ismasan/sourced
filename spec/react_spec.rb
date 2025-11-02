# frozen_string_literal: true

require 'spec_helper'

class ReactTestReactor
  include Sourced::React

  Event1 = Sourced::Message.define('reacttest.event1')
  Event2 = Sourced::Message.define('reacttest.event2')
  Event3 = Sourced::Message.define('reacttest.event3')
  Event4 = Sourced::Message.define('reacttest.event4')
  Event5 = Sourced::Message.define('reacttest.event5')
  Cmd1 = Sourced::Message.define('reacttest.cmd1') do
    attribute :name, String
  end
  Cmd2 = Sourced::Message.define('reacttest.cmd2')
  Cmd3 = Sourced::Message.define('reacttest.cmd3')
  NotifyWildcardReaction = Sourced::Message.define('reacttest.NotifyWildcardReaction') do
    attribute :state
    attribute :event
  end

  def state = { name: 'test' }

  def self.handled_messages_for_evolve = [Event1, Event4, Event5]

  reaction Event1 do |state, event|
    dispatch(Cmd1, name: state[:name]).to(event)
  end

  reaction Event2 do |_state, event|
    dispatch(Cmd2)
    dispatch(Cmd3)
      .with_metadata(greeting: 'Hi!')
      .at(Time.now + 10)
  end

  reaction Event3 do |_state, _event|
    nil
  end

  # This wildcard reaction will be registered 
  # for all events present in .handled_messages_for_evolve
  # that do not have custom reactions
  reaction do |state, event|
    dispatch NotifyWildcardReaction, state:, event:
  end
end

RSpec.describe Sourced::React do
  specify '.handled_messages_for_react' do
    expect(ReactTestReactor.handled_messages_for_react).to eq([
      ReactTestReactor::Event1,
      ReactTestReactor::Event2,
      ReactTestReactor::Event3,
      ReactTestReactor::Event4,
      ReactTestReactor::Event5
    ])
  end

  describe '#react' do
    it 'returns messages to append or schedule' do
      now = Time.now
      Timecop.freeze(now) do
        evt1 = ReactTestReactor::Event1.new(stream_id: '1', seq: 1)
        evt2 = ReactTestReactor::Event2.new(stream_id: '1', seq: 2)
        commands = ReactTestReactor.new.react([evt1, evt2])
        expect(commands.map(&:class)).to eq([
          ReactTestReactor::Cmd1,
          ReactTestReactor::Cmd2,
          ReactTestReactor::Cmd3
        ])
        expect(commands.map { |e| e.metadata[:producer] }).to eq(%w[ReactTestReactor ReactTestReactor ReactTestReactor])
        expect(commands.first.causation_id).to eq(evt1.id)
        expect(commands.first.created_at).to eq(now)
        expect(commands.first.payload.name).to eq('test')
        expect(commands.last.causation_id).to eq(evt2.id)
        expect(commands.last.metadata[:greeting]).to eq('Hi!')
        expect(commands.last.created_at).to eq(now + 10)
      end
    end

    it 'runs wildcard reactions' do
      evt4 = ReactTestReactor::Event4.new(stream_id: '1', seq: 1)
      commands = ReactTestReactor.new.react(evt4)
      expect(commands.map(&:class)).to eq([ReactTestReactor::NotifyWildcardReaction])
      expect(commands.first.payload.state[:name]).to eq('test')
      expect(commands.first.payload.event).to eq(evt4)
    end
  end
end
