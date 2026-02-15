# frozen_string_literal: true

module Sourced
  module Backends
    class SequelBackend
      # PostgreSQL LISTEN/NOTIFY transport for real-time message dispatch.
      #
      # Uses a dedicated PG connection to subscribe to the +sourced_new_messages+
      # channel. Two kinds of signal are multiplexed over this channel:
      #
      # - **Type notifications** ({#notify}): comma-separated message type strings
      #   (e.g. +"orders.created,orders.shipped"+). Routed to the +on_append+ callback.
      # - **Reactor notifications** ({#notify_reactor}): prefixed with +"reactor:"+
      #   followed by a consumer group ID (e.g. +"reactor:OrderReactor"+).
      #   Routed to the +on_resume+ callback.
      #
      # Implements the same interface as {Sourced::InlineNotifier}.
      #
      # @example Wiring in a Dispatcher
      #   pg_notifier = PGNotifier.new(db: Sequel.postgres('mydb'))
      #   pg_notifier.on_append(queuer)
      #   pg_notifier.on_resume(queuer.method(:queue_reactor))
      #
      #   # In a dedicated fiber:
      #   pg_notifier.start   # blocks, listening for notifications
      #
      #   # From the append path (inside a transaction):
      #   pg_notifier.notify(['orders.created', 'orders.shipped'])
      #
      #   # From start_consumer_group:
      #   pg_notifier.notify_reactor('OrderReactor')
      class PGNotifier
        # @return [String] PG NOTIFY channel name
        CHANNEL = 'sourced_new_messages'

        REACTOR_PREFIX = 'reactor:'

        # @param db [Sequel::Database] a PostgreSQL Sequel database connection.
        #   The LISTEN connection should be separate from the one used for writes
        #   to avoid blocking.
        def initialize(db:)
          @db = db
          @listening = false
        end

        # Register a callback for new-message notifications.
        #
        # @param callable [#call] receives an +Array<String>+ of message type strings
        # @return [void]
        def on_append(callable)
          @on_append_callback = callable
        end

        # Register a callback for reactor-resume notifications.
        #
        # @param callable [#call] receives a group_id +String+
        # @return [void]
        def on_resume(callable)
          @on_resume_callback = callable
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

        # Fire a PG NOTIFY to signal that a reactor has been resumed.
        # Payload is prefixed with +"reactor:"+ to distinguish from type notifications.
        #
        # @param group_id [String] consumer group ID of the resumed reactor
        # @return [void]
        def notify_reactor(group_id)
          @db.run(Sequel.lit("SELECT pg_notify('#{CHANNEL}', ?)", "#{REACTOR_PREFIX}#{group_id}"))
        end

        # Block on PG LISTEN, dispatching to the appropriate callback per notification.
        # Loops with a 2-second timeout so {#stop} is checked periodically.
        #
        # @return [void]
        def start
          @listening = true
          while @listening
            @db.listen(CHANNEL, timeout: 2) do |_ch, _pid, payload|
              if payload.start_with?(REACTOR_PREFIX)
                group_id = payload.delete_prefix(REACTOR_PREFIX)
                @on_resume_callback&.call(group_id)
              else
                types = payload.split(',').map(&:strip)
                @on_append_callback&.call(types)
              end
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
