# frozen_string_literal: true

module Sourced
  # Default backend notifier for non-PG backends (TestBackend, SQLite).
  # A simple synchronous pub/sub: {#publish} invokes all subscribers inline.
  # +start+ and +stop+ are no-ops since there is no background listener.
  #
  # Implements the same interface as {Backends::SequelBackend::PGNotifier}.
  #
  # @example
  #   notifier = InlineNotifier.new
  #   notifier.subscribe(->(event, value) { puts "#{event}: #{value}" })
  #   notifier.notify_new_messages(['orders.created', 'orders.paid'])
  #   notifier.notify_reactor_resumed('OrderReactor')
  class InlineNotifier
    def initialize
      @subscribers = []
    end

    # Register a subscriber to receive all published events.
    #
    # @param callable [#call] receives +(event_name, value)+ where both are Strings
    # @return [void]
    def subscribe(callable)
      @subscribers << callable
    end

    # Publish an event to all subscribers synchronously.
    #
    # @param event_name [String] event name (e.g. +'messages_appended'+)
    # @param value [String] event payload
    # @return [void]
    def publish(event_name, value)
      @subscribers.each { |s| s.call(event_name, value) }
    end

    # Notify that new messages were appended.
    #
    # @param types [Array<String>] message type strings
    # @return [void]
    def notify_new_messages(types)
      publish('messages_appended', types.uniq.join(','))
    end

    # Notify that a stopped reactor has been resumed.
    #
    # @param group_id [String] consumer group ID of the resumed reactor
    # @return [void]
    def notify_reactor_resumed(group_id)
      publish('reactor_resumed', group_id)
    end

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def start = nil

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def stop = nil
  end
end
