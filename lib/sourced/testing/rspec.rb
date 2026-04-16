# frozen_string_literal: true

require 'sourced'
require 'sourced/store'

module Sourced
  module Testing
    module RSpec
      NONE = [].freeze

      # Entry point for reactors GWT tests.
      #
      # Works with any reactor that responds to the standard
      # <tt>handle_batch(partition_values, new_messages, history:, replaying:)</tt>
      # contract (Deciders, Projectors, DurableWorkflows).
      #
      # @param reactor_class [Class] a reactors class
      # @param partition_attrs [Hash] partition key-value pairs (e.g. device_id: 'd1')
      # @return [GWT]
      #
      # @example Decider
      #   with_reactor(MyDecider, device_id: 'd1')
      #     .given(DeviceRegistered, device_id: 'd1', name: 'Sensor')
      #     .when(BindDevice, device_id: 'd1', asset_id: 'a1')
      #     .then(DeviceBound, device_id: 'd1', asset_id: 'a1')
      #
      # @example Projector — assert state via block
      #   with_reactor(MyProjector, list_id: 'L1')
      #     .given(ItemAdded, list_id: 'L1', name: 'Apple')
      #     .then { |r| expect(r.state[:items]).to eq(['Apple']) }
      def with_reactor(reactor_class, **partition_attrs)
        GWT.new(reactor_class, **partition_attrs)
      end

      # Uniform result yielded to +.then+ / +.then!+ block callbacks.
      # Gives access to both the reactor's produced action pairs / messages
      # (what deciders typically assert on) and the evolved state (what
      # projectors typically assert on).
      RunResult = Data.define(:pairs, :messages, :state)

      class MessageMatcher
        def initialize(expected_messages)
          @expected_messages = Array(expected_messages)
          @errors = []
          @mismatching = Hash.new { |h, k| h[k] = [] }
        end

        def matches?(actual_messages)
          if @expected_messages.size != actual_messages.size
            @errors << "Expected #{@expected_messages.size} messages, but got #{actual_messages.size}"
            @errors << actual_messages.inspect
            return false
          end

          @expected_messages.each.with_index do |expected, idx|
            actual = actual_messages[idx]
            @mismatching[idx] << "expected a #{expected.class}, got #{actual.class}" unless actual.class == expected.class
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

      class GWT
        def initialize(reactor_class, **partition_attrs)
          @reactor_class = reactor_class
          @partition_values = partition_attrs
          @given_messages = []
          @when_messages = []
          @asserted = false
        end

        # Accumulate history / context messages. These become +history.messages+
        # passed to the reactor's +handle_batch+.
        #
        # @param klass_or_instance [Class, Sourced::Message]
        # @param payload [Hash]
        # @return [self]
        def given(klass_or_instance = nil, **payload)
          raise 'test case already asserted' if @asserted

          @given_messages << build_message(klass_or_instance, **payload)
          self
        end

        alias_method :and, :given

        # The batch of new messages to process via +handle_batch+. Can be
        # called multiple times to supply several messages.
        #
        # @param klass_or_instance [Class, Sourced::Message]
        # @param payload [Hash]
        # @return [self]
        def when(klass_or_instance = nil, **payload)
          raise 'test case already asserted' if @asserted

          @when_messages << build_message(klass_or_instance, **payload)
          self
        end

        # Assert expected outcomes.
        #
        #   - Pass message class + payload pairs (or instances) to assert
        #     produced messages.
        #   - Pass +[]+ or +NONE+ to assert no messages.
        #   - Pass an Exception class (+ optional message) to assert the
        #     reactor raised.
        #   - Pass a block to receive a {RunResult} for custom assertions.
        #
        # @return [self]
        def then(*expected, **payload, &block)
          run_then(false, *expected, **payload, &block)
        end

        # Like #then, but runs Sync / AfterSync actions before computing
        # the result yielded to the block (or before extracting messages).
        def then!(*expected, **payload, &block)
          run_then(true, *expected, **payload, &block)
        end

        private

        def build_message(klass_or_instance, **payload)
          if klass_or_instance.is_a?(Sourced::Message)
            klass_or_instance
          else
            klass_or_instance.new(payload: payload)
          end
        end

        def run_then(sync, *expected, **payload, &block)
          @asserted = true

          # Shorthand: .then(Class, key: val) → build message from class + payload
          if expected.size == 1 && expected[0].is_a?(Class) && !(expected[0] < ::Exception) && payload.any?
            expected = [expected[0].new(payload: payload)]
          end

          # Exception expectation
          if expected.size >= 1 && exception_expectation?(expected[0])
            expect_exception(expected[0], expected[1])
            return self
          end

          pairs = run_handle_batch

          if sync
            pairs.each do |actions, _|
              Array(actions).select { |a|
                a.is_a?(Sourced::Actions::Sync) || a.is_a?(Sourced::Actions::AfterSync)
              }.each(&:call)
            end
          end

          if block_given?
            block.call(RunResult.new(pairs: pairs, messages: extract_messages(pairs), state: compute_state(sync: sync)))
            return self
          end

          actual_messages = extract_messages(pairs)
          expected_msgs = build_expected(*expected)

          if expected_msgs.empty?
            unless actual_messages.empty?
              ::RSpec::Expectations.fail_with(
                "Expected no messages, but got #{actual_messages.size}: #{actual_messages.inspect}"
              )
            end
            return self
          end

          matcher = MessageMatcher.new(expected_msgs)
          unless matcher.matches?(actual_messages)
            ::RSpec::Expectations.fail_with(matcher.failure_message)
          end

          self
        end

        def run_handle_batch
          guard = Sourced::ConsistencyGuard.new(conditions: [], last_position: 0)
          history = Sourced::ReadResult.new(messages: @given_messages, guard: guard)
          @reactor_class.handle_batch(
            @partition_values,
            @when_messages,
            history: history,
            replaying: false
          )
        end

        # Build an instance and evolve it with all known messages so the
        # caller can assert on state regardless of reactor type. For reactors
        # whose +handle_batch+ evolves its own instance (Decider, Projector,
        # DurableWorkflow), this is an independent, predictable computation.
        # When +sync+ is true, also runs the reactor's Sync / AfterSync
        # blocks against this instance so their state mutations are visible.
        def compute_state(sync: false)
          instance = @reactor_class.new(@partition_values)
          return nil unless instance.respond_to?(:evolve)

          messages = @given_messages + @when_messages
          instance.evolve(messages)
          run_sync_on(instance, messages) if sync
          instance.state
        end

        # Invoke Sync / AfterSync blocks against +instance+. Per-block
        # kwarg signatures vary by reactor type (deciders expect +events:+,
        # projectors expect +replaying:+); we inspect each block's
        # parameters and pass only what it declares.
        def run_sync_on(instance, messages)
          all_args = { state: instance.state, messages: messages, events: [], replaying: false }
          klass = instance.class
          blocks = []
          blocks.concat(klass.sync_blocks) if klass.respond_to?(:sync_blocks)
          blocks.concat(klass.after_sync_blocks) if klass.respond_to?(:after_sync_blocks)
          blocks.each do |block|
            wanted = block.parameters.select { |type, _| type == :keyreq || type == :key }.map(&:last)
            instance.instance_exec(**all_args.slice(*wanted), &block)
          end
        end

        def extract_messages(pairs)
          pairs.flat_map { |actions, _|
            Array(actions)
              .select { |a| a.respond_to?(:messages) }
              .flat_map(&:messages)
          }
        end

        def build_expected(*args)
          return [] if args == [[]] || args == [NONE]
          return [] if args.empty?

          args.map do |arg|
            case arg
            when Sourced::Message
              arg
            else
              raise ArgumentError, "unsupported expected message: #{arg.inspect}"
            end
          end
        end

        def exception_expectation?(arg)
          arg.is_a?(Class) && arg < ::Exception
        end

        def expect_exception(exception_class, message = nil)
          begin
            run_handle_batch
          rescue exception_class => e
            if message && e.message != message
              ::RSpec::Expectations.fail_with(
                "expected #{exception_class} with message #{message.inspect}, " \
                "but got #{e.message.inspect}"
              )
            end
            return
          end

          ::RSpec::Expectations.fail_with("expected #{exception_class} to be raised, but nothing was raised")
        end
      end
    end
  end
end
