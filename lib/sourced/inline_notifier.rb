# frozen_string_literal: true

module Sourced
  # Default backend notifier for non-PG backends (TestBackend, SQLite).
  # Invokes callbacks synchronously. +start+ and +stop+ are no-ops
  # since there is no background listener.
  #
  # Implements the same interface as {Backends::SequelBackend::PGNotifier}:
  # +on_append+, +on_resume+, +notify+, +notify_reactor+, +start+, +stop+.
  #
  # @example
  #   notifier = InlineNotifier.new
  #   notifier.on_append(->(types) { puts types })
  #   notifier.on_resume(->(group_id) { puts group_id })
  #   notifier.notify(['orders.created'])
  #   notifier.notify_reactor('OrderReactor')
  class InlineNotifier
    # Register a callback to be invoked when new messages are appended.
    #
    # @param callable [#call] receives an Array of unique type strings
    # @return [void]
    def on_append(callable)
      @on_append_callback = callable
    end

    # Register a callback to be invoked when a reactor is resumed.
    #
    # @param callable [#call] receives a group_id String
    # @return [void]
    def on_resume(callable)
      @on_resume_callback = callable
    end

    # Invoke the +on_append+ callback synchronously with deduplicated types.
    #
    # @param types [Array<String>] message type strings
    # @return [void]
    def notify(types)
      @on_append_callback&.call(types.uniq)
    end

    # Invoke the +on_resume+ callback synchronously with the given group_id.
    #
    # @param group_id [String] consumer group ID of the resumed reactor
    # @return [void]
    def notify_reactor(group_id)
      @on_resume_callback&.call(group_id)
    end

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def start = nil

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def stop = nil
  end
end
