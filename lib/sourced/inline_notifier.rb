# frozen_string_literal: true

module Sourced
  # Default backend notifier for non-PG backends (TestBackend, SQLite).
  # Invokes the +on_append+ callback synchronously within +notify+.
  # +start+ and +stop+ are no-ops since there is no background listener.
  #
  # Implements the same interface as {Backends::SequelBackend::PGNotifier}:
  # +on_append+, +notify+, +start+, +stop+.
  #
  # @example
  #   notifier = InlineNotifier.new
  #   notifier.on_append(->(types) { puts types })
  #   notifier.notify(['orders.created', 'orders.created'])
  #   # prints: ["orders.created"]
  class InlineNotifier
    # Register a callback to be invoked when new messages are appended.
    #
    # @param callable [#call] receives an Array of unique type strings
    # @return [void]
    def on_append(callable)
      @on_append_callback = callable
    end

    # Invoke the +on_append+ callback synchronously with deduplicated types.
    #
    # @param types [Array<String>] message type strings
    # @return [void]
    def notify(types)
      @on_append_callback&.call(types.uniq)
    end

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def start = nil

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def stop = nil
  end
end
