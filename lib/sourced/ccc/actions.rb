# frozen_string_literal: true

module Sourced
  module CCC
    module Actions
      OK = :ok
      RETRY = :retry

      # Append messages to the CCC store with optional consistency guard.
      # Auto-correlates messages with the source message at execution time.
      class Append
        attr_reader :messages, :guard

        def initialize(messages, guard: nil)
          @messages = Array(messages)
          @guard = guard
        end

        # @param store [CCC::Store]
        # @param source_message [CCC::Message] message to correlate from
        # @return [Array<CCC::Message>] correlated messages that were appended
        def execute(store, source_message)
          correlated = messages.map { |m| source_message.correlate(m) }
          store.append(correlated, guard: guard)
          correlated
        end
      end

      # Execute a synchronous side effect within the current transaction.
      class Sync
        def initialize(work)
          @work = work
        end

        def call = @work.call

        def execute(_store, _source_message)
          call
          nil
        end
      end
    end
  end
end
