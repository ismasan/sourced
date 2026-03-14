# frozen_string_literal: true

require 'sourced/ccc'

module Sourced
  module CCC
    module Testing
      module RSpec
        NONE = [].freeze

        # Entry point for CCC reactor GWT tests.
        #
        # @param reactor_class [Class] a CCC::Decider or CCC::Projector subclass
        # @param partition_attrs [Hash] partition key-value pairs (e.g. device_id: 'd1')
        # @return [GWT]
        #
        # @example Decider
        #   with_reactor(MyDecider, device_id: 'd1')
        #     .given(DeviceRegistered, device_id: 'd1', name: 'Sensor')
        #     .when(BindDevice, device_id: 'd1', asset_id: 'a1')
        #     .then(DeviceBound, device_id: 'd1', asset_id: 'a1')
        #
        # @example Projector
        #   with_reactor(MyProjector, list_id: 'L1')
        #     .given(ItemAdded, list_id: 'L1', name: 'Apple')
        #     .then { |state| expect(state[:items]).to eq(['Apple']) }
        def with_reactor(reactor_class, **partition_attrs)
          GWT.new(reactor_class, **partition_attrs)
        end

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
            @partition_attrs = partition_attrs
            @partition_values = partition_attrs
            @given_messages = []
            @when_message = nil
            @asserted = false
          end

          # Accumulate history/context messages.
          # For Deciders: these become the history (ReadResult).
          # For Projectors: these are evolved onto the instance.
          #
          # @param klass [Class] message class
          # @param payload [Hash] payload attributes
          # @return [self]
          def given(klass_or_instance = nil, **payload)
            raise 'test case already asserted' if @asserted

            msg = build_message(klass_or_instance, **payload)
            @given_messages << msg
            self
          end

          alias_method :and, :given

          # Set the command to decide on (Deciders only).
          #
          # @param klass [Class] command class
          # @param payload [Hash] payload attributes
          # @return [self]
          def when(klass_or_instance = nil, **payload)
            raise 'test case already asserted' if @asserted
            raise ArgumentError, '.when is not supported for Projectors' if projector?

            @when_message = build_message(klass_or_instance, **payload)
            self
          end

          # Assert expected outcomes.
          #
          # For Deciders:
          #   - Pass message class + payload pairs to assert produced messages
          #   - Pass [] or NONE to assert no messages
          #   - Pass an Exception class (+ optional message) to assert invariant violation
          #   - Pass a block to receive action pairs for custom assertions
          #
          # For Projectors:
          #   - Requires a block that receives the evolved state
          #
          # @return [self]
          def then(*expected, **payload, &block)
            run_then(false, *expected, **payload, &block)
          end

          # Like #then, but runs sync actions before yielding state (Projectors)
          # or before extracting messages (Deciders).
          def then!(*expected, **payload, &block)
            run_then(true, *expected, **payload, &block)
          end

          private

          def decider?
            @reactor_class < CCC::Decider
          end

          def projector?
            @reactor_class < CCC::Projector
          end

          def build_message(klass_or_instance, **payload)
            if klass_or_instance.is_a?(CCC::Message)
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

            if decider?
              run_decider_then(sync, *expected, &block)
            elsif projector?
              run_projector_then(sync, *expected, &block)
            else
              raise ArgumentError, "unsupported reactor type: #{@reactor_class}"
            end

            self
          end

          def run_decider_then(sync, *expected, &block)
            # Exception expectation
            if expected.size >= 1 && exception_expectation?(expected[0])
              expect_exception(expected[0], expected[1])
              return
            end

            pairs = run_decider

            if sync
              pairs.each do |actions, _|
                Array(actions).select { |a|
                  a.is_a?(CCC::Actions::Sync) || a.is_a?(CCC::Actions::AfterSync)
                }.each(&:call) # collect_actions produces both types; filter from pairs
              end
            end

            if block_given?
              block.call(pairs)
              return
            end

            # Extract messages from action pairs
            actual_messages = extract_messages(pairs)

            # Build expected messages
            expected_msgs = build_expected(*expected)

            if expected_msgs.empty?
              unless actual_messages.empty?
                ::RSpec::Expectations.fail_with(
                  "Expected no messages, but got #{actual_messages.size}: #{actual_messages.inspect}"
                )
              end
              return
            end

            matcher = MessageMatcher.new(expected_msgs)
            unless matcher.matches?(actual_messages)
              ::RSpec::Expectations.fail_with(matcher.failure_message)
            end
          end

          def run_projector_then(sync, *_expected, &block)
            raise ArgumentError, '.then for Projectors requires a block' unless block_given?

            instance = @reactor_class.new(@partition_values)

            if @reactor_class < CCC::Projector::EventSourced
              # EventSourced: evolve from full history
              instance.evolve(@given_messages)
            else
              # StateStored: evolve from given messages
              instance.evolve(@given_messages)
            end

            if sync
              instance.collect_actions(
                state: instance.state, messages: @given_messages, replaying: false
              ).each(&:call)
            end

            block.call(instance.state)
          end

          def run_decider
            guard = ConsistencyGuard.new(conditions: [], last_position: 0)
            history = ReadResult.new(messages: @given_messages, guard: guard)
            @reactor_class.handle_batch(@partition_values, [@when_message], history: history)
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
              when CCC::Message
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
            guard = ConsistencyGuard.new(conditions: [], last_position: 0)
            history = ReadResult.new(messages: @given_messages, guard: guard)

            begin
              @reactor_class.handle_batch(@partition_values, [@when_message], history: history)
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
end
