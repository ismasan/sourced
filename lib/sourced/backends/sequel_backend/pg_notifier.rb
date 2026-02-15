# frozen_string_literal: true

module Sourced
  module Backends
    class SequelBackend
      # PostgreSQL LISTEN/NOTIFY transport for real-time message dispatch.
      #
      # Uses a dedicated PG connection to subscribe to the +sourced_new_messages+
      # channel. When messages are appended to the event store, {#notify} fires
      # +pg_notify+ inside the current transaction (delivered atomically on commit).
      # The {#start} loop receives those notifications and invokes the registered
      # +on_append+ callback (typically a {Sourced::Dispatcher::NotificationQueuer}).
      #
      # Implements the same interface as {Sourced::InlineNotifier}:
      # +on_append+, +notify+, +start+, +stop+.
      #
      # @example Wiring in a Dispatcher
      #   pg_notifier = PGNotifier.new(db: Sequel.postgres('mydb'))
      #   pg_notifier.on_append(queuer)  # a Dispatcher::NotificationQueuer
      #
      #   # In a dedicated fiber:
      #   pg_notifier.start   # blocks, listening for notifications
      #
      #   # From the append path (inside a transaction):
      #   pg_notifier.notify(['orders.created', 'orders.shipped'])
      #
      #   # On shutdown:
      #   pg_notifier.stop
      class PGNotifier
        # @return [String] PG NOTIFY channel name
        CHANNEL = 'sourced_new_messages'

        # @param db [Sequel::Database] a PostgreSQL Sequel database connection.
        #   The LISTEN connection should be separate from the one used for writes
        #   to avoid blocking.
        def initialize(db:)
          @db = db
          @listening = false
        end

        # Register a callback to be invoked when a notification is received.
        #
        # @param callable [#call] receives an +Array<String>+ of message type strings
        # @return [void]
        def on_append(callable)
          @on_append_callback = callable
        end

        # Fire a PG NOTIFY with the given message types. Should be called inside
        # a transaction so the notification is delivered atomically on commit.
        #
        # @param types [Array<String>] message type strings to broadcast
        # @return [void]
        def notify(types)
          types_str = types.uniq.join(',')
          @db.run(Sequel.lit("SELECT pg_notify('#{CHANNEL}', ?)", types_str))
        end

        # Block on PG LISTEN, invoking the +on_append+ callback for each notification.
        # Loops with a 2-second timeout so {#stop} is checked periodically.
        #
        # @return [void]
        def start
          @listening = true
          while @listening
            @db.listen(CHANNEL, timeout: 2) do |_ch, _pid, payload|
              types = payload.split(',').map(&:strip)
              @on_append_callback&.call(types)
            end
          end
        end

        # Signal the LISTEN loop to exit after the current timeout cycle.
        #
        # @return [void]
        def stop
          @listening = false
        end
      end
    end
  end
end
