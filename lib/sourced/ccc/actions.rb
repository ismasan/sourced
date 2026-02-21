# frozen_string_literal: true

module Sourced
  module CCC
    module Actions
      OK = :ok
      RETRY = :retry

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

        def initialize(messages, guard: nil, source: nil, correlated: false)
          @messages = Array(messages)
          @guard = guard
          @source = source
          @correlated = correlated
        end

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
