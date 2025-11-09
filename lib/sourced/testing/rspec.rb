# frozen_string_literal: true

require 'sourced/message'

# An RSpec module with helpers to test Sourced reactors
# RSpec.describe Payment do
#   subject(:payment) { Payment.new(id: 'payment-1') }
#
#   it 'starts a payment' do
#     with_actor(payment)
#       .when(Payment::Start, order_id: 'o1', amount: 1000)
#       .then(Payment::Started.build(payment.id, order_id: 'o1', amount: 1000))
#   end
#
#   it 'is a no-op if payment already started' do
#     with_actor(payment)
#       .given(Payment::Started, order_id: 'o1', amount: 1000)
#       .when(Payment::Start, order_id: 'o1', amount: 1000)
#       .then([])
#   end
#
#   it 'confirms a started payment' do
#     with_actor(payment)
#       .given(Payment::Started, order_id: 'o1', amount: 1000)
#       .when(Payment::Confirm)
#       .then(Payment::Confirmed.build(payment.id))
#   end
#
#   it 'does not confirm a payment that has not started' do
#     with_actor(payment)
#       .when(Payment::Confirm)
#       .then([])
#   end
# end
module Sourced
  module Testing
    module RSpec
      class MessageMatcher
        MessageArray = Sourced::Types::Array[Sourced::Message]

        def initialize(expected_messages)
          @expected_messages = expected_messages
          @errors = []
          @mismatching = Hash.new { |h, k| h[k] = [] }
        end

        def matches?(actual_messages)
          if !(MessageArray === actual_messages)
            @errors << "expected an array of Sourced messages, but got #{actual_messages.inspect}"
            return false
          end

          if @expected_messages.size != actual_messages.size
            @errors << "Expected #{@expected_messages.size} messages, but got #{actual_messages.size}"
            @errors << actual_messages.inspect
            return false
          end

          @expected_messages.each.with_index do |expected, idx|
            actual = actual_messages[idx]
            @mismatching[idx] << "expected a #{expected.class}, got #{actual.class}" unless actual.class === expected
            @mismatching[idx] << "expected stream_id '#{expected.stream_id}', got '#{actual.stream_id}'" unless expected.stream_id == actual.stream_id
            @mismatching[idx] << "expected payload #{expected.payload.to_h.inspect}, got #{actual.payload.to_h.inspect}" unless expected.payload == actual.payload
          end

          return false if @mismatching.any?

          true
        end

        def failure_message
          err = +@errors.join("\n")
          @mismatching.each do |idx, errors|
            err << "Message #{idx}: \n"
            errors.each do |e|
              err << "- #{e}\n"
            end
            err << "\n"
          end
          err
        end
      end

      def match_sourced_messages(expected_messages)
        MessageMatcher.new(expected_messages)
      end

      class GWT
        def initialize(actor)
          @actor = actor
          @when = nil
        end

        def given(*args)
          message = build_message(*args)
          @actor.evolve(message)
          self
        end

        def when(*args)
          @when = build_message(*args)
          self
        end

        def and(...) = given(...)

        def then(*expected)
          expected = build_messages(*expected)
          actual = @actor.decide(@when)
          matcher = MessageMatcher.new(expected)
          if !matcher.matches?(actual)
            ::RSpec::Expectations.fail_with(matcher.failure_message)
          end
        end

        private

        ERROR = proc do |args|
          raise ArgumentError, "no support for #{args.inspect}"
        end

        NONE = [].freeze

        def build_messages(*messages)
          messages = build_message(*messages) do |arr|
            Array(arr).map { |e| build_message(*e) }
          end
          Array(messages)
        end

        def build_message(*args, &fallback)
          fallback ||= ERROR

          case args
          in [Class => klass, Hash => payload]
            klass.build(@actor.id, payload)
          in [Class => klass]
            klass.build(@actor.id)
          in [Sourced::Message => mm]
            mm
          in [NONE]
            NONE
          else
            fallback.(args)
          end
        end
      end

      # Make a base Reactor .handle() interface support #evolve, #decide
      class ReactorAdapter
        attr_reader :id

        def initialize(reactor, id)
          @reactor = reactor
          @id = id
          @history = []
        end

        def evolve(events)
          @history += Array(events)
        end

        def decide(command)
          actions = @reactor.handle(command, history: [*@history, command])
          messages_from_actions(actions)
        end

        private

        def messages_from_actions(actions)
          actions.each.with_object([]) do |action, list|
            list.concat(action.messages) if action.respond_to?(:messages)
          end
        end
      end

      ActorInterface = Sourced::Types::Interface[:decide, :evolve]

      def with_reactor(*args)
        actor = case args
        in [ActorInterface => a]
          a
        in [Sourced::ReactorInterface => reactor, String => id]
          ReactorAdapter.new(reactor, id)
        in [Sourced::ReactorInterface => reactor]
          ReactorAdapter.new(reactor, Sourced.new_stream_id)
        end

        GWT.new(actor)
      end
    end
  end
end
