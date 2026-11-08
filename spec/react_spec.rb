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

  # Unwraps through to_message, without overriding #class.
  Wrapped = Struct.new(:to_message) do
    def payload = to_message.payload
  end
end

RSpec.describe Sourced::React do
  let(:reactor_class) do
    Class.new do
      include Sourced::React
      extend Sourced::Consumer

      def context
        {}
      end

      reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext do |_context, msg|
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

  describe 'wrapped messages' do
    it 'reacts to a wrapper by its to_message class, passing the wrapper' do
      instance = reactor_class.new
      wrapped = ReactTestMessages::Wrapped.new(ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' }))

      expect(instance.reacts_to?(wrapped)).to be(true)
      expect(instance.react(wrapped).map { |msg| msg.payload.thing_id }).to eq(['t1'])
      expect(instance.reacts_to?(ReactTestMessages::Wrapped.new(ReactTestMessages::Unhandled.new(payload: { foo: 'x' })))).to be(false)
    end
  end

  describe '.reaction' do
    it 'takes anything whose to_message_class is a message class, and reacts to that class' do
      declared = Object.new
      declared.define_singleton_method(:to_message_class) { ReactTestMessages::Unhandled }
      reactor_class.reaction(declared => ReactTestMessages::DoNext) { |_context, msg| ReactTestMessages::DoNext.new(payload: { thing_id: msg.payload.foo }) }

      expect(reactor_class.handled_messages_for_react).to include(ReactTestMessages::Unhandled)
      result = reactor_class.new.react(ReactTestMessages::Unhandled.new(payload: { foo: 'f1' }))
      expect(result.map { |msg| msg.payload.thing_id }).to eq(['f1'])
    end

    it 'takes only what answers to_message_class' do
      expect {
        reactor_class.reaction(:react_test_something_happened => nil) { |_context, _msg| nil }
      }.to raise_error(NoMethodError, /to_message_class/)
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
      expect(subclass.reaction_outputs).to eq(reactor_class.reaction_outputs)
    end
  end

  describe 'declared outputs' do
    let(:happened) { ReactTestMessages::SomethingHappened.new(payload: { thing_id: 't1' }) }

    def reactor(&block)
      Class.new do
        include Sourced::React
        extend Sourced::Consumer

        consumer_group 'react_outputs'

        def context = {}

        class_exec(&block)
      end
    end

    it 'records each reaction\'s outputs, and their union' do
      klass = reactor do
        reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext | nil do |*|
          nil
        end
        reaction ReactTestMessages::MultiReaction | ReactTestMessages::AnotherMultiReaction =>
                   ReactTestMessages::DelayedCommand | ReactTestMessages::DoNext do |*|
          nil
        end
      end

      expect(klass.reaction_outputs[ReactTestMessages::SomethingHappened])
        .to have_attributes(messages: [ReactTestMessages::DoNext], optional: true)
      expect(klass.reaction_outputs[ReactTestMessages::AnotherMultiReaction].messages)
        .to eq([ReactTestMessages::DelayedCommand, ReactTestMessages::DoNext])
      expect(klass.dispatched_messages).to eq([ReactTestMessages::DoNext, ReactTestMessages::DelayedCommand])
    end

    it 'raises for a dispatched message it does not declare' do
      klass = reactor do
        reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext do |_context, msg|
          dispatch ReactTestMessages::DelayedCommand, thing_id: msg.payload.thing_id
        end
      end

      expect { klass.new.react(happened) }
        .to raise_error(Sourced::React::UndeclaredMessageError, /appended .*DelayedCommand, which it doesn't declare/)
    end

    it 'raises for a returned message it does not declare' do
      klass = reactor do
        reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext do |*|
          ReactTestMessages::DelayedCommand.new(payload: { thing_id: 't1' })
        end
      end

      expect { klass.new.react(happened) }.to raise_error(Sourced::UndeclaredOutputError, /DelayedCommand/)
    end

    it 'raises when it appends nothing without | nil, and allows it with' do
      strict = reactor { reaction(ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext) { |*| nil } }
      lenient = reactor { reaction(ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext | nil) { |*| nil } }

      expect { strict.new.react(happened) }
        .to raise_error(Sourced::React::UndeclaredMessageError, /appended nothing; declare .*DoNext \| nil/)
      expect(lenient.new.react(happened)).to eq([])
    end

    it 'declares outputs for a catch-all reaction with any:' do
      klass = reactor do
        def self.evolve_selector = Sourced::Selector[ReactTestMessages::Wildcarded]

        reaction any: ReactTestMessages::DoNext do |_context, msg|
          dispatch ReactTestMessages::DoNext, thing_id: msg.payload.thing_id
        end
      end

      expect(klass.reaction_outputs[ReactTestMessages::Wildcarded].messages).to eq([ReactTestMessages::DoNext])
      expect(klass.new.react(ReactTestMessages::Wildcarded.new(payload: { thing_id: 'w' })).map(&:class))
        .to eq([ReactTestMessages::DoNext])
    end

    it 'subscribes without a handler for a bare class, with no block' do
      klass = reactor { reaction ReactTestMessages::SomethingHappened }

      expect(klass.handled_messages_for_react).to eq([ReactTestMessages::SomethingHappened])
      expect(klass.new.react(happened)).to eq([])
    end

    it 'raises for a block without declared outputs, a pair without a block, or several pairs' do
      expect { reactor { reaction(ReactTestMessages::SomethingHappened) { |*| nil } } }
        .to raise_error(ArgumentError, /declares what it appends: one pair/)
      expect { reactor { reaction { |*| nil } } }.to raise_error(ArgumentError, /any: Command/)
      expect { reactor { reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext } }
        .to raise_error(ArgumentError, /needs a block/)
      expect {
        reactor do
          reaction(ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext,
                   ReactTestMessages::Unhandled => ReactTestMessages::DoNext) { |*| nil }
        end
      }.to raise_error(ArgumentError, /one pair/)
      expect { reactor { reaction(ReactTestMessages::SomethingHappened => 'nope') { |*| nil } } }
        .to raise_error(ArgumentError, /isn't a message class/)
    end
  end

  describe 'dispatch DSL' do
    let(:dsl_reactor_class) do
      Class.new do
        include Sourced::React
        extend Sourced::Consumer

        consumer_group 'ccc_reactor'

        def context
          { source: 'context' }
        end

        def self.evolve_selector = Sourced::Selector[ReactTestMessages::Wildcarded]

        reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext do |_context, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
        end

        reaction ReactTestMessages::DelayedCommand => ReactTestMessages::DelayedCommand do |_context, msg|
          dispatch(ReactTestMessages::DelayedCommand, thing_id: msg.payload.thing_id)
            .with_metadata(foo: 'bar')
            .at(Time.now + 10)
        end

        reaction ReactTestMessages::MultiReaction | ReactTestMessages::AnotherMultiReaction => ReactTestMessages::DoNext do |_context, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: msg.payload.thing_id)
        end

        reaction any: ReactTestMessages::DoNext do |context, msg|
          dispatch(ReactTestMessages::DoNext, thing_id: "#{context[:source]}-#{msg.payload.thing_id}")
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
        expect(result.first.metadata[:producer]).to eq('ccc_reactor')
        expect(result.first.metadata[:foo]).to eq('bar')
        expect(result.first.created_at).to eq((now + 10).floor(6))
      end
    end

    it 'supports dispatching multiple messages from one reaction block' do
      klass = Class.new do
        include Sourced::React
        extend Sourced::Consumer

        def context
          {}
        end

        reaction ReactTestMessages::SomethingHappened => ReactTestMessages::DoNext | ReactTestMessages::DelayedCommand do |_context, msg|
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
      expect(result.first.payload.thing_id).to eq('context-t1')
      expect(dsl_reactor_class.catch_all_react_events).to eq(Set[
        ReactTestMessages::Wildcarded
      ])
    end

    it 'reacts to every evolved type with a catch-all, whatever the partitioning' do
      queue = Class.new(Sourced::Projector::StateStored) do
        exclusive
        consumer_group 'react_test.catch_all_queue'
        evolve(ReactTestMessages::Wildcarded) { |*| }
        reaction(any: nil) { |*| }
      end
      early = Class.new(Sourced::Decider) do
        evolve(ReactTestMessages::Wildcarded) { |*| }
        reaction(any: nil) { |*| }
        partition_by :thing_id
      end

      expect(queue.catch_all_react_events).to eq(Set[ReactTestMessages::Wildcarded])
      expect(early.catch_all_react_events).to eq(Set[ReactTestMessages::Wildcarded])
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
