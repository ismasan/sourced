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

  class Order < Sourced::Actor
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

    command :start_payment do |_, cmd|
      if state[:name]
        event :payment_started
      end
    end

    event :payment_started

    reaction :payment_started do |_, evt|
      dispatch(Payment::Process).to("#{evt.stream_id}-payment")
    end
  end

  class Payment < Sourced::Actor
    command :process do |_, cmd|
      event :processed
    end

    event :processed
  end

  class Telemetry
    STREAM_ID = 'telemetry-stream'
    include Sourced::Handler

    Logged = Sourced::Message.define('test-telemetry.logged') do
      attribute :source_stream, String
      attribute :message_type, String
    end

    consumer do |c|
      c.group_id = 'test-telemetry'
    end

    on Order::PaymentStarted, Payment::Processed do |event|
      logged = Logged.build(STREAM_ID, source_stream: event.stream_id, message_type: event.type)
      [logged]
    end
  end
end

RSpec.describe Sourced::Testing::RSpec do
  include described_class

  describe 'with_reactor' do
    context 'with Reactor interface' do
      it 'works' do
        with_reactor(Testing::Reactor, 'a')
          .when(Testing::Start, name: 'Joe')
          .then(Testing::Started.build('a', name: 'Joe'))

        with_reactor(Testing::Reactor, 'a')
          .given(Testing::Started, name: 'Joe')
          .when(Testing::Start, name: 'Joe')
          .then([])

        # If supports any .handle() interface, including u classes
        with_reactor(Testing::Order, 'a')
          .when(Testing::Start, name: 'Joe')
          .then(Testing::Started.build('a', name: 'Joe'))
      end
    end

    context 'with Actor instance' do
      it 'works' do
        with_reactor(Testing::Order.new(id: 'a'))
          .when(Testing::Start, name: 'Joe')
          .then(Testing::Started.build('a', name: 'Joe'))

        with_reactor(Testing::Order.new(id: 'a'))
          .when(Testing::Start, name: 'Joe')
          .then(Testing::Started, name: 'Joe')

        with_reactor(Testing::Order.new(id: 'a'))
          .when(Testing::Start, name: 'Joe')
          .then([Testing::Started.build('a', name: 'Joe')])

        with_reactor(Testing::Order.new(id: 'a'))
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

  describe 'with_reactors' do
    it 'tests collaboration of reactors' do
      order_stream = 'actor-1'
      payment_stream = 'actor-1-payment'
      telemetry_stream = Testing::Telemetry::STREAM_ID

      # With these reactors
      with_reactors(Testing::Order, Testing::Payment, Testing::Telemetry)
        # GIVEN that these events exist in history
        .given(Testing::Started.build(order_stream, name: 'foo'))
        # WHEN I dispatch this new command
        .when(Testing::Order::StartPayment.build(order_stream))
        # Then I expect
        .then do |stage|
          # The different reactors collaborated and
          # left this message trail behind
          # Backend#messages is only available in the TestBackend
          expect(stage.backend.messages).to match_sourced_messages([
            Testing::Started.build(order_stream, name: 'foo'), 
            Testing::Order::StartPayment.build(order_stream), 
            Testing::Order::PaymentStarted.build(order_stream), 
            Testing::Telemetry::Logged.build(telemetry_stream, source_stream: order_stream, message_type: 'testing.order.payment_started'),
            Testing::Payment::Process.build(payment_stream), 
            Testing::Payment::Processed.build(payment_stream),
            Testing::Telemetry::Logged.build(telemetry_stream, source_stream: payment_stream, message_type: 'testing.payment.processed'),
          ])
        end
    end
  end
end
