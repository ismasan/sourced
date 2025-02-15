# frozen_string_literal: true

require 'sequel'
require 'thread'
require 'json'

module Sourced
  module Backends
    # a PubSub implementation using Postgres' LISTEN/NOTIFY
    # @example
    #
    #   # Publisher
    #   Sourced.config.backend.pub_sub.publish('my_channel', event)
    #
    #   # Subscriber
    #   channel = Source.config.backend.pub_sub.subscribe('my_channel')
    #   channel.start do |event, ch|
    #     case event
    #     when MyEvent
    #       # do something
    #     end
    #   end
    #
    class SequelPubSub
      # @option db [Sequel::Database]
      def initialize(db:)
        @db = db
      end

      # @param channel_name [String]
      # @return [Channel]
      def subscribe(channel_name)
        Channel.new(db: @db, name: channel_name)
      end

      # @param channel_name [String]
      # @param event [Sourced::Message]
      # @return [self]
      def publish(channel_name, event)
        event_data = JSON.dump(event.to_h)
        @db.run(Sequel.lit('SELECT pg_notify(?, ?)', channel_name, event_data))
        self
      end
    end

    class Channel
      NOTIFY_CHANNEL = 'sourced-scheduler-ch'

      attr_reader :name

      # @option db [Sequel::Database]
      # @option name [String]
      # @option timeout [Numeric]
      def initialize(db:, name: NOTIFY_CHANNEL, timeout: 3)
        @db = db
        @name = name
        @running = false
        @timeout = timeout
      end

      # Start listening to incoming events
      # via Postgres LISTEN as implemented in Sequel
      #
      # @option handler [#call, nil] a callable object to use as an event handler
      # @yieldparam [Sourced::Message]
      # @yieldparam [Channel]
      # @return [self]
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
          # Perhaps that's enough
        end

        self
      end

      # Mark the channel to stop on the next tick
      def stop
        @running = false
      end

      # Public so that we can test this separately from async channel listening.
      def parse(payload)
        data = JSON.parse(payload, symbolize_names: true)
        Sourced::Message.from(data)
      end
    end
  end
end
