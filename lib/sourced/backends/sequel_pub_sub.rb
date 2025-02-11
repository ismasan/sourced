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

      def subscribe(channel_name)
        Channel.new(db: @db, name: channel_name)
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

      def initialize(db:, name: NOTIFY_CHANNEL, timeout: 3)
        @db = db
        @name = name
        @running = false
        @timeout = timeout
      end

      def start(handler: nil, &block)
        return self if @running

        handler ||= block

        @running = true
        # We need a reasobaly short timeout
        # so that this block gets a chance to check the @running flag
        @db.listen(@name, timeout: @timeout, loop: true) do |_channel, _pid, payload|
          break unless @running

          handler.call parse(payload), self
          # TODO: handle exceptions
          # Any exception raised here will be rescued by Sequel
          # and close the LISTEN connection
          # Perhaps that's enaough
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
