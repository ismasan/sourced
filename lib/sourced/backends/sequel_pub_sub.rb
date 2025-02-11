# frozen_string_literal: true

require 'sequel'
require 'thread'
require 'json'

module Sourced
  module Backends
    class SequelPubSub
      def initialize(db:)
        @db = db
      end

      def subscribe(channel_name, handler = nil, &block)
        Channel.new(db: @db, name: channel_name, handler: handler || block).start
      end

      def publish(channel_name, event)
        event_data = JSON.dump(event.to_h)
        @db.run(Sequel.lit('SELECT pg_notify(?, ?)', channel_name, event_data))
        self
      end
    end

    class Channel
      NOTIFY_CHANNEL = 'sourced-scheduler-ch'

      attr_reader :name

      def initialize(db:, handler:, name: NOTIFY_CHANNEL)
        @db = db
        @name = name
        @handler = handler
        @running = false
      end

      def start
        return self if @running

        @running = true
        @db.listen(@name, loop: true) do |_channel, _pid, payload|
          @handler.call parse(payload), self
          break unless @running
          # TODO: handle exceptions
        end

        self
      end

      # Public so that we can test this separately from async channel listening.
      def parse(payload)
        data = JSON.parse(payload, symbolize_names: true)
        Sourced::Message.from(data)
      end

      def stop
        @running = false
      end
    end
  end
end
