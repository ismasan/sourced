# frozen_string_literal: true

module Sourced
  # Action builders and declarative action signals for reactors.
  #
  # Actions are *inert data*: they describe an intent (append these messages,
  # schedule those, run this side effect, delete the source on ack) but never
  # touch the store themselves. {Sourced::ActionRunner} is the only code
  # that routes a signal to a store operation.
  #
  # Every action value object implements +deconstruct_keys+ so it pattern-matches
  # identically to the equivalent plain Hash. This lets third-party reactors
  # (e.g. Sidereal Commanders) return plain Hash signals without depending on
  # Sourced's classes:
  #
  #   { type: :append, messages: [...], delete: true }
  #
  # matches the same interpreter branch as +Sourced::Actions::Append.new(...)+.
  module Actions
    RETRY = :retry

    # A no-op finalize signal: acknowledge the source message with no side effects.
    # Destructures to +{ type: :ack, delete: false }+ like any other signal, so the
    # interpreter needs no special case for it.
    class Ack
      def deconstruct_keys(_keys)
        { type: :ack, delete: false }
      end
    end

    OK = Ack.new.freeze

    # Wrap produced messages in a single append action. Scheduling is handled by
    # the store: {Sourced::Store#append} defers any future-dated message (built
    # with {Sourced::Message#at}) into the scheduled_messages table, so there is
    # no separate schedule action here.
    #
    # @param messages [Sourced::Message, Array<Sourced::Message>] messages produced by a reactor
    # @param guard [ConsistencyGuard, nil] optional concurrency guard
    # @param source [Sourced::Message, nil] source message used for correlation when executing
    # @param delete [Boolean] whether the source message should be deleted on ack
    # @return [Array<Append>] a one-element array (empty when no messages)
    def self.build_for(messages, guard: nil, source: nil, delete: false)
      messages = Array(messages)
      messages.empty? ? [] : [Append.new(messages, guard:, source:, delete:)]
    end

    # Append messages to the store with optional consistency guard.
    # Correlation happens in the interpreter at execution time.
    #
    # When +source:+ is provided, it overrides the runtime's source_message
    # for correlation (e.g. reactions correlated with the event, not the command).
    class Append
      attr_reader :messages, :guard, :source, :delete

      # @param messages [Sourced::Message, Array<Sourced::Message>] messages to append
      # @param guard [ConsistencyGuard, nil] optional optimistic concurrency guard
      # @param source [Sourced::Message, nil] explicit correlation source
      # @param delete [Boolean] delete the source message on ack
      def initialize(messages, guard: nil, source: nil, delete: false)
        @messages = Array(messages)
        @guard = guard
        @source = source
        @delete = delete
      end

      def deconstruct_keys(_keys)
        { type: :append, messages: @messages, guard: @guard, source: @source, delete: @delete }
      end
    end

    # Execute a synchronous side effect within the current transaction.
    class Sync
      attr_reader :work

      # @param work [#call] callable to execute
      def initialize(work)
        @work = work
      end

      # @return [Object] the callable's return value
      def call = @work.call

      def deconstruct_keys(_keys)
        { type: :sync, work: @work }
      end
    end

    # Execute a side effect after the transaction commits.
    class AfterSync
      attr_reader :work

      # @param work [#call] callable to execute
      def initialize(work)
        @work = work
      end

      # @return [Object] the callable's return value
      def call = @work.call

      def deconstruct_keys(_keys)
        { type: :after_sync, work: @work }
      end
    end
  end
end
