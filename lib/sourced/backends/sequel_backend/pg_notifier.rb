# frozen_string_literal: true

module Sourced
  module Backends
    class SequelBackend
      # PostgreSQL LISTEN/NOTIFY pub/sub transport for real-time message dispatch.
      #
      # Events are multiplexed over a single PG channel. The wire format is
      # +event_name:value+ (split on the first colon).
      #
      # Implements the same interface as {Sourced::InlineNotifier}.
      #
      # @example Wiring in a Dispatcher
      #   pg_notifier = PGNotifier.new(db: Sequel.postgres('mydb'))
      #   pg_notifier.subscribe(queuer)
      #
      #   # In a dedicated fiber:
      #   pg_notifier.start   # blocks, listening for notifications
      #
      #   # From the append path (inside a transaction):
      #   pg_notifier.notify_new_messages(['orders.created', 'orders.shipped'])
      #
      #   # From start_consumer_group:
      #   pg_notifier.notify_reactor_resumed('OrderReactor')
      class PGNotifier
        # @return [String] PG NOTIFY channel name
        CHANNEL = 'sourced_new_messages'

        # @param db [Sequel::Database] a PostgreSQL Sequel database connection.
        #   The LISTEN connection should be separate from the one used for writes
        #   to avoid blocking.
        def initialize(db:)
          @db = db
          @subscribers = []
          @listening = false
        end

        # Register a subscriber to receive all published events.
        #
        # @param callable [#call] receives +(event_name, value)+ where both are Strings
        # @return [void]
        def subscribe(callable)
          @subscribers << callable
        end

        # Publish an event via PG NOTIFY. Should be called inside a transaction
        # so the notification is delivered atomically on commit.
        # Wire format: +event_name:value+.
        #
        # @param event_name [String] event name
        # @param value [String] event payload
        # @return [void]
        def publish(event_name, value)
          @db.run(Sequel.lit("SELECT pg_notify('#{CHANNEL}', ?)", "#{event_name}:#{value}"))
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

        # Block on PG LISTEN, dispatching events to all subscribers.
        # Loops with a 2-second timeout so {#stop} is checked periodically.
        #
        # @return [void]
        def start
          @listening = true
          while @listening
            @db.listen(CHANNEL, timeout: 2) do |_ch, _pid, payload|
              event_name, value = payload.split(':', 2)
              @subscribers.each { |s| s.call(event_name, value) }
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
