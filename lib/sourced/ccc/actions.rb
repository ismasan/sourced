# frozen_string_literal: true

module Sourced
  module CCC
    # Action builders and executable action types for CCC reactors.
    module Actions
      OK = :ok
      RETRY = :retry

      # Split produced messages into immediate append actions and delayed schedule actions.
      #
      # @param messages [CCC::Message, Array<CCC::Message>] messages produced by a reactor
      # @param guard [ConsistencyGuard, nil] optional concurrency guard for immediate appends
      # @param source [CCC::Message, nil] source message used for correlation when executing
      # @param correlated [Boolean] whether +messages+ are already correlated
      # @return [Array<Append, Schedule>] executable actions in append/schedule groups
      def self.build_for(messages, guard: nil, source: nil, correlated: false)
        actions = []
        messages = Array(messages)
        return actions if messages.empty?

        now = Time.now
        to_schedule, to_append = messages.partition { |message| message.created_at > now }

        actions << Append.new(to_append, guard:, source:, correlated:) if to_append.any?
        to_schedule.group_by(&:created_at).each do |at, scheduled_messages|
          actions << Schedule.new(scheduled_messages, at:, source:, correlated:)
        end

        actions
      end

      # Append messages to the CCC store with optional consistency guard.
      # Auto-correlates messages with the source message at execution time.
      #
      # When +source:+ is provided, it overrides the runtime's source_message
      # for correlation (e.g. reactions correlated with the event, not the command).
      #
      # When +correlated: true+, messages are assumed to be already correlated
      # and are appended as-is without re-correlation.
      class Append
        attr_reader :messages, :guard, :source

        # @param messages [CCC::Message, Array<CCC::Message>] messages to append
        # @param guard [ConsistencyGuard, nil] optional optimistic concurrency guard
        # @param source [CCC::Message, nil] explicit correlation source
        # @param correlated [Boolean] whether +messages+ are already correlated
        def initialize(messages, guard: nil, source: nil, correlated: false)
          @messages = Array(messages)
          @guard = guard
          @source = source
          @correlated = correlated
        end

        # @return [Boolean] whether messages should be appended without re-correlation
        def correlated? = @correlated

        # @param store [CCC::Store]
        # @param source_message [CCC::Message] default message to correlate from
        # @return [Array<CCC::Message>] correlated messages that were appended
        def execute(store, source_message)
          to_append = if @correlated
            messages
          else
            correlate_from = @source || source_message
            messages.map { |m| correlate_from.correlate(m) }
          end
          store.append(to_append, guard: guard)
          to_append
        end
      end

      # Schedule messages for future promotion into the main CCC log.
      class Schedule
        attr_reader :messages, :at, :source

        # @param messages [CCC::Message, Array<CCC::Message>] messages to schedule
        # @param at [Time] when the messages should become available for promotion
        # @param source [CCC::Message, nil] explicit correlation source
        # @param correlated [Boolean] whether +messages+ are already correlated
        def initialize(messages, at:, source: nil, correlated: false)
          @messages = Array(messages)
          @at = at
          @source = source
          @correlated = correlated
        end

        # @return [Boolean] whether messages should be scheduled without re-correlation
        def correlated? = @correlated

        # @param store [CCC::Store]
        # @param source_message [CCC::Message] default message to correlate from
        # @return [Array<CCC::Message>] correlated messages that were scheduled
        def execute(store, source_message)
          to_schedule = if @correlated
            messages
          else
            correlate_from = @source || source_message
            messages.map { |message| correlate_from.correlate(message) }
          end
          store.schedule_messages(to_schedule, at: at)
          to_schedule
        end
      end

      # Execute a synchronous side effect within the current transaction.
      class Sync
        # @param work [#call] callable to execute
        def initialize(work)
          @work = work
        end

        # @return [Object] the callable's return value
        def call = @work.call

        # @param _store [Object] unused
        # @param _source_message [Object] unused
        # @return [nil]
        def execute(_store, _source_message)
          call
          nil
        end
      end
    end
  end
end
