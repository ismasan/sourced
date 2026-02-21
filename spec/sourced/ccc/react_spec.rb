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
end

RSpec.describe Sourced::CCC::React do
  let(:reactor_class) do
    Class.new do
      include Sourced::CCC::React

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
end
