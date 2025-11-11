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
#
#   # Evaluating block with .then
#   it 'calls API' do
#     with_actor(payment)
#       .when(Payment::Confirm)
#       .then do |actions|
#         expect(api_request).to have_been_requested
#       end
#   end
# end
module Sourced
  module Testing
    module RSpec
      ERROR = proc do |args|
        raise ArgumentError, "no support for #{args.inspect}"
      end

      NONE = [].freeze

      module MessageBuilder
        private

        def stream_id = nil

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
            klass.build(stream_id, payload)
          in [Class => klass]
            klass.build(stream_id)
          in [Sourced::Message => mm]
            mm
          in [NONE]
            NONE
          else
            fallback.(args)
          end
        end

        def run_sync_actions(actions)
          return if @sync_run

          actions.filter { |a| Sourced::Actions::Sync === a }.each(&:call)
          @sync_run = true
        end

        def partition_actions(actions)
          actions.partition do |a|
            a.respond_to?(:messages)
          end
        end
      end

      class MessageMatcher
        MessageArray = Sourced::Types::Array[Sourced::Message]

        def initialize(expected_messages)
          @expected_messages = Array(expected_messages)
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

      FinishedTestCase = Class.new(StandardError)

      class GWT
        include MessageBuilder

        def initialize(actor)
          @actor = actor
          @when = nil
          @actual = nil
          @sync_run = false
        end

        def given(*args)
          raise FinishedTestCase, 'test case already asserted, cannot add more events to it' if @actual

          message = build_message(*args)
          @actor.evolve(message)
          self
        end

        def when(*args)
          raise FinishedTestCase, 'test case already asserted, cannot add more events to it' if @actual

          @when = build_message(*args)
          self
        end

        def and(...) = given(...)

        # Like #then, but run any sync actions
        def then!(*expected, &block)
          run_then(true, *expected, &block)
        end

        def then(*expected, &block)
          run_then(false, *expected, &block)
        end

        private

        def stream_id = @actor.id

        def run_then(sync, *expected, &block)
          # Actor instances maintain their own state (#given above calls #evolve on them)
          # ReactorAdapter also keeps its own history via #evolve
          # So here we satisfy the expected :history arg, but don't provide historical messages
          @actual ||= @actor.handle(@when, history: [])
          actions_with_messages, actions_without_messages = partition_actions(@actual)
          run_sync_actions(actions_without_messages) if sync

          if block_given?
            block.call(@actual)
            return self
          end

          expected = build_messages(*expected)
          matcher = MessageMatcher.new(expected)
          actual_messages = actions_with_messages.flat_map(&:messages)
          if !matcher.matches?(actual_messages)
            ::RSpec::Expectations.fail_with(matcher.failure_message)
          end

          self
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

        # Include :history for compatibility
        # but we keep out own history
        def handle(command, history: [])
          @reactor.handle(command, history: [*@history, command])
        end
      end

      ActorInterface = Sourced::Types::Interface[:decide, :evolve, :handle]

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

      ReactorsArray = Sourced::Types::Array[Sourced::ReactorInterface]

      class Stage
        include MessageBuilder

        attr_reader :router

        def initialize(reactors, router: nil, logger: Logger.new(nil))
          @reactors = ReactorsArray.parse(reactors)
          @router ||= Sourced::Router.new(
            backend: Sourced::Backends::TestBackend.new, 
            logger:
          )
          @reactors.each do |r|
            @router.register(r)
          end
          @stream_id = nil
          @called = false
          @when = nil
          @sync_run = false
        end

        def with_stream(stream_id)
          @stream_id = stream_id
          self
        end

        def given(*args)
          raise FinishedTestCase, 'test case already asserted, cannot add more events to it' if @called

          message = build_message(*args)
          @router.backend.append_next_to_stream(message.stream_id, [message])
          self
        end

        def when(*args)
          raise FinishedTestCase, 'test case already asserted, cannot add more events to it' if @called

          @when = build_message(*args)
          @router.backend.append_next_to_stream(@when.stream_id, [@when])
          self
        end

        def and(...) = given(...)

        def run
          @called = true
          pid = Process.pid
          have_messages = @reactors.each.with_index.with_object({}) { |(_, i), m| m[i] = true }

          loop do
            @reactors.each.with_index do |r, idx|
              found = @router.handle_next_event_for_reactor(r, worker_id: pid)
              have_messages[idx] = found
            end
            break if have_messages.values.none?
          end

          self
        end

        # Like #then, but run any sync actions
        def then!(&block)
          self.then(&block)
        end

        def then(&block)
          run

          if block_given?
            block.call(self)
            return self
          end

          self
        end

        def backend = router.backend

        private

        attr_reader :stream_id
      end

      def with_reactors(*reactors)
        Stage.new(reactors)
      end
    end
  end
end
