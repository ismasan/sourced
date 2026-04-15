# frozen_string_literal: true

require 'spec_helper'
require 'sourced'

module ReactTestMessages
  SomethingHappened = Sourced::Message.define('react_test.something.happened') do
    attribute :thing_id, String
  end

  DoNext = Sourced::Message.define('react_test.do_next') do
    attribute :thing_id, String
  end

  Unhandled = Sourced::Message.define('react_test.unhandled') do
    attribute :foo, String
  end

  Wildcarded = Sourced::Message.define('react_test.wildcarded') do
    attribute :thing_id, String
  end

  DelayedCommand = Sourced::Message.define('react_test.delayed.command') do
    attribute :thing_id, String
  end

  MultiReaction = Sourced::Message.define('react_test.multi.reaction') do
    attribute :thing_id, String
  end

  AnotherMultiReaction = Sourced::Message.define('react_test.another.multi.reaction') do
    attribute :thing_id, String
  end
end

RSpec.describe Sourced::React do
  let(:reactor_class) do
    Class.new do
      include Sourced::React
      extend Sourced::Consumer

      def state
        {}
      end

      reaction ReactTestMessages::SomethingHappened do |_state, msg|
        ReactTestMessages::DoNext.new(payload: { thing_id: msg.payload.thing_id })
      end
    end
  end

  describe '#react' do
    it 'returns raw messages (not correlated)' do
      instance = reactor_class.new
      msg = ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })

      result = instance.react(msg)

      expect(result.size).to eq(1)
      expect(result.first).to be_a(ReactTestMessages::DoNext)
      expect(result.first.payload.thing_id).to eq('t1')
      # Not correlated — causation_id is its own id
      expect(result.first.causation_id).to eq(result.first.id)
    end

    it 'accepts a single message or an array of messages' do
      instance = reactor_class.new
      msg = ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })

      expect(instance.react([msg]).map(&:class)).to eq([ReactTestMessages::DoNext])
    end

    it 'returns empty array for unregistered types' do
      instance = reactor_class.new
      msg = ReactTestMessages::Unhandled.new(payload: { foo: 'bar' })

      result = instance.react(msg)
      expect(result).to eq([])
    end
  end

  describe '#reacts_to?' do
    it 'returns true for registered types' do
      instance = reactor_class.new
      msg = ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })
      expect(instance.reacts_to?(msg)).to be true
    end

    it 'returns false for unregistered types' do
      instance = reactor_class.new
      msg = ReactTestMessages::Unhandled.new(payload: { foo: 'bar' })
      expect(instance.reacts_to?(msg)).to be false
    end
  end

  describe '.handled_messages_for_react' do
    it 'tracks registered classes' do
      expect(reactor_class.handled_messages_for_react).to contain_exactly(
        ReactTestMessages::SomethingHappened
      )
    end
  end

  describe 'inheritance' do
    it 'subclass inherits reaction handlers' do
      subclass = Class.new(reactor_class)
      expect(subclass.handled_messages_for_react).to contain_exactly(
        ReactTestMessages::SomethingHappened
      )

      instance = subclass.new
      msg = ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })
      result = instance.react(msg)
      expect(result.size).to eq(1)
    end
  end

  describe 'dispatch DSL' do
    let(:dsl_reactor_class) do
      Class.new do
        include Sourced::React
        extend Sourced::Consumer

        consumer_group 'ccc-reactor'

        def state
          { source: 'state' }
        end

        def self.handled_messages_for_evolve
          [ReactTestMessages::Wildcarded]
        end

        reaction ReactTestMessages::SomethingHappened do |_state, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
        end

        reaction :react_test_delayed_command do |_state, msg|
          dispatch(:react_test_delayed_command, thing_id: msg.payload.thing_id)
            .with_metadata(foo: 'bar')
            .at(Time.now + 10)
        end

        reaction ReactTestMessages::MultiReaction, ReactTestMessages::AnotherMultiReaction do |_state, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
        end

        reaction do |state, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: "#{state[:source]}-#{msg.payload.thing_id}")
        end
      end
    end

    it 'supports dispatch with correlation, producer metadata, metadata chaining, and delays' do
      now = Time.now
      Timecop.freeze(now) do
        instance = dsl_reactor_class.new
        source = ReactTestMessages::DelayedCommand.new(payload: { thing_id: 't1' })

        result = instance.react(source)

        expect(result.map(&:class)).to eq([ReactTestMessages::DelayedCommand])
        expect(result.first.causation_id).to eq(source.id)
        expect(result.first.correlation_id).to eq(source.correlation_id)
        expect(result.first.metadata[:producer]).to eq('ccc-reactor')
        expect(result.first.metadata[:foo]).to eq('bar')
        expect(result.first.created_at).to eq(now + 10)
      end
    end

    it 'supports dispatching multiple messages from one reaction block' do
      klass = Class.new do
        include Sourced::React
        extend Sourced::Consumer

        def state
          {}
        end

        reaction ReactTestMessages::SomethingHappened do |_state, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
          dispatch(ReactTestMessages::DelayedCommand, thing_id: msg.payload.thing_id)
        end
      end

      result = klass.new.react(
        ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })
      )

      expect(result.map(&:class)).to eq([
        ReactTestMessages::DoNext,
        ReactTestMessages::DelayedCommand
      ])
    end

    it 'supports wildcard reactions for evolve types without explicit handlers' do
      result = dsl_reactor_class.new.react(
        ReactTestMessages::Wildcarded.new(payload: { thing_id: 't1' })
      )

      expect(result.map(&:class)).to eq([ReactTestMessages::DoNext])
      expect(result.first.payload.thing_id).to eq('state-t1')
      expect(dsl_reactor_class.catch_all_react_events).to eq(Set[
        ReactTestMessages::Wildcarded
      ])
    end

    it 'supports reactions registered for multiple message classes' do
      instance = dsl_reactor_class.new

      first = instance.react(ReactTestMessages::MultiReaction.new(payload: { thing_id: 't1' }))
      second = instance.react(ReactTestMessages::AnotherMultiReaction.new(payload: { thing_id: 't2' }))

      expect(first.map(&:class)).to eq([ReactTestMessages::DoNext])
      expect(second.map(&:class)).to eq([ReactTestMessages::DoNext])
    end
  end
end
