# frozen_string_literal: true

module Sourced
  module Backends
    class SequelBackend
      # PG LISTEN/NOTIFY transport for real-time message dispatch.
      # notify() fires pg_notify inside the current transaction.
      # start() blocks on LISTEN, invoking the on_append callback per notification.
      class PGNotifier
        CHANNEL = 'sourced_new_messages'

        def initialize(db:)
          @db = db
          @listening = false
        end

        def on_append(callable)
          @on_append_callback = callable
        end

        def notify(types)
          types_str = types.uniq.join(',')
          @db.run(Sequel.lit("SELECT pg_notify('#{CHANNEL}', ?)", types_str))
        end

        def start
          @listening = true
          @db.listen(CHANNEL, timeout: 2, loop: true) do |_ch, _pid, payload|
            break unless @listening

            types = payload.split(',').map(&:strip)
            @on_append_callback&.call(types)
          end
        end

        def stop
          @listening = false
        end
      end
    end
  end
end
