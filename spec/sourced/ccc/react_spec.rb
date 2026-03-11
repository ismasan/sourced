# frozen_string_literal: true

require 'spec_helper'
require 'sourced/ccc'

module CCCReactTestMessages
  SomethingHappened = Sourced::CCC::Message.define('react_test.something.happened') do
    attribute :thing_id, String
  end

  DoNext = Sourced::CCC::Message.define('react_test.do_next') do
    attribute :thing_id, String
  end

  Unhandled = Sourced::CCC::Message.define('react_test.unhandled') do
    attribute :foo, String
  end

  Wildcarded = Sourced::CCC::Message.define('react_test.wildcarded') do
    attribute :thing_id, String
  end

  DelayedCommand = Sourced::CCC::Message.define('react_test.delayed.command') do
    attribute :thing_id, String
  end

  MultiReaction = Sourced::CCC::Message.define('react_test.multi.reaction') do
    attribute :thing_id, String
  end

  AnotherMultiReaction = Sourced::CCC::Message.define('react_test.another.multi.reaction') do
    attribute :thing_id, String
  end
end

RSpec.describe Sourced::CCC::React do
  let(:reactor_class) do
    Class.new do
      include Sourced::CCC::React
      extend Sourced::CCC::Consumer

      def state
        {}
      end

      reaction CCCReactTestMessages::SomethingHappened do |_state, msg|
        CCCReactTestMessages::DoNext.new(payload: { thing_id: msg.payload.thing_id })
      end
    end
  end

  describe '#react' do
    it 'returns raw messages (not correlated)' do
      instance = reactor_class.new
      msg = CCCReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })

      result = instance.react(msg)

      expect(result.size).to eq(1)
      expect(result.first).to be_a(CCCReactTestMessages::DoNext)
      expect(result.first.payload.thing_id).to eq('t1')
      # Not correlated — causation_id is its own id
      expect(result.first.causation_id).to eq(result.first.id)
    end

    it 'accepts a single message or an array of messages' do
      instance = reactor_class.new
      msg = CCCReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })

      expect(instance.react([msg]).map(&:class)).to eq([CCCReactTestMessages::DoNext])
    end

    it 'returns empty array for unregistered types' do
      instance = reactor_class.new
      msg = CCCReactTestMessages::Unhandled.new(payload: { foo: 'bar' })

      result = instance.react(msg)
      expect(result).to eq([])
    end
  end

  describe '#reacts_to?' do
    it 'returns true for registered types' do
      instance = reactor_class.new
      msg = CCCReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })
      expect(instance.reacts_to?(msg)).to be true
    end

    it 'returns false for unregistered types' do
      instance = reactor_class.new
      msg = CCCReactTestMessages::Unhandled.new(payload: { foo: 'bar' })
      expect(instance.reacts_to?(msg)).to be false
    end
  end

  describe '.handled_messages_for_react' do
    it 'tracks registered classes' do
      expect(reactor_class.handled_messages_for_react).to contain_exactly(
        CCCReactTestMessages::SomethingHappened
      )
    end
  end

  describe 'inheritance' do
    it 'subclass inherits reaction handlers' do
      subclass = Class.new(reactor_class)
      expect(subclass.handled_messages_for_react).to contain_exactly(
        CCCReactTestMessages::SomethingHappened
      )

      instance = subclass.new
      msg = CCCReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })
      result = instance.react(msg)
      expect(result.size).to eq(1)
    end
  end

  describe 'dispatch DSL' do
    let(:dsl_reactor_class) do
      Class.new do
        include Sourced::CCC::React
        extend Sourced::CCC::Consumer

        consumer_group 'ccc-reactor'

        def state
          { source: 'state' }
        end

        def self.handled_messages_for_evolve
          [CCCReactTestMessages::Wildcarded]
        end

        reaction CCCReactTestMessages::SomethingHappened do |_state, msg|
          dispatch(CCCReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
        end

        reaction :react_test_delayed_command do |_state, msg|
          dispatch(:react_test_delayed_command, thing_id: msg.payload.thing_id)
            .with_metadata(foo: 'bar')
            .at(Time.now + 10)
        end

        reaction CCCReactTestMessages::MultiReaction, CCCReactTestMessages::AnotherMultiReaction do |_state, msg|
          dispatch(CCCReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
        end

        reaction do |state, msg|
          dispatch(CCCReactTestMessages::DoNext, thing_id: "#{state[:source]}-#{msg.payload.thing_id}")
        end
      end
    end

    it 'supports dispatch with correlation, producer metadata, metadata chaining, and delays' do
      now = Time.now
      Timecop.freeze(now) do
        instance = dsl_reactor_class.new
        source = CCCReactTestMessages::DelayedCommand.new(payload: { thing_id: 't1' })

        result = instance.react(source)

        expect(result.map(&:class)).to eq([CCCReactTestMessages::DelayedCommand])
        expect(result.first.causation_id).to eq(source.id)
        expect(result.first.correlation_id).to eq(source.correlation_id)
        expect(result.first.metadata[:producer]).to eq('ccc-reactor')
        expect(result.first.metadata[:foo]).to eq('bar')
        expect(result.first.created_at).to eq(now + 10)
      end
    end

    it 'supports dispatching multiple messages from one reaction block' do
      klass = Class.new do
        include Sourced::CCC::React
        extend Sourced::CCC::Consumer

        def state
          {}
        end

        reaction CCCReactTestMessages::SomethingHappened do |_state, msg|
          dispatch(CCCReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
          dispatch(CCCReactTestMessages::DelayedCommand, thing_id: msg.payload.thing_id)
        end
      end

      result = klass.new.react(
        CCCReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' })
      )

      expect(result.map(&:class)).to eq([
        CCCReactTestMessages::DoNext,
        CCCReactTestMessages::DelayedCommand
      ])
    end

    it 'supports wildcard reactions for evolve types without explicit handlers' do
      result = dsl_reactor_class.new.react(
        CCCReactTestMessages::Wildcarded.new(payload: { thing_id: 't1' })
      )

      expect(result.map(&:class)).to eq([CCCReactTestMessages::DoNext])
      expect(result.first.payload.thing_id).to eq('state-t1')
      expect(dsl_reactor_class.catch_all_react_events).to eq(Set[
        CCCReactTestMessages::Wildcarded
      ])
    end

    it 'supports reactions registered for multiple message classes' do
      instance = dsl_reactor_class.new

      first = instance.react(CCCReactTestMessages::MultiReaction.new(payload: { thing_id: 't1' }))
      second = instance.react(CCCReactTestMessages::AnotherMultiReaction.new(payload: { thing_id: 't2' }))

      expect(first.map(&:class)).to eq([CCCReactTestMessages::DoNext])
      expect(second.map(&:class)).to eq([CCCReactTestMessages::DoNext])
    end
  end
end
