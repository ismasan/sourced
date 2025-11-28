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
    class PGPubSub
      class Listener
        def initialize(db:, channel_name:, timeout: 2, logger:)
          @db = db
          @channel_name = channel_name
          @logger = logger
          @channels = {}
          @timeout = timeout
          @queue = Sourced.config.executor.new_queue
          @running = true
          @info = "[#{[self.class.name, @channel_name, Process.pid, object_id].join(' ')}]"
        end

        def subscribe(channel)
          start
          @queue << [:subscribe, channel]
          self
        end

        def unsubscribe(channel)
          @queue << [:unsubscribe, channel]
          self
        end

        def start
          return if @control

          @control = Sourced.config.executor.start(wait: false) do |t|
            t.spawn do
              while (msg = @queue.pop)
                case msg
                in :stop
                  @running = false
                  # Stop all channels?
                  @logger.info "#{@info} stopping"
                  @channels.values.each(&:stop)
                in [:unsubscribe, channel]
                  if @channels.delete(channel.object_id)
                    @logger.info "#{@info} unsubscribe channel #{channel.object_id}"
                    if @channels.empty?
                      @logger.info "#{@info} all channels unsubscribed."
                    end
                  end
                in [:subscribe, channel]
                  @logger.info "#{@info} subscribe channel #{channel.object_id}"
                  @channels[channel.object_id] ||= channel
                in [:dispatch, message]
                  @channels.values.each { |ch| ch << message }
                end
              end

              @logger.info "#{@info} Stopped"
            end

            t.spawn do
              @db.listen(@channel_name, timeout: @timeout, loop: true) do |_channel, _pid, payload|
                break unless @running

                message = parse(payload)
                @queue << [:dispatch, message]
              end
            end
          end

          @logger.info "#{@info} Started"
        end

        def stop
          @queue << :stop
          @control&.wait
          @control = nil
          @logger.info "#{@info} Stopped"
          self
        end

        private def parse(payload)
          data = JSON.parse(payload, symbolize_names: true)
          Sourced::Message.from(data)
        end
      end

      # @option db [Sequel::Database]
      def initialize(db:, logger:)
        @db = db
        @logger = logger
        @mutex = Mutex.new
        @listeners ||= {}
      end

      # @param channel_name [String]
      # @return [Channel]
      def subscribe(channel_name)
        @mutex.synchronize do
          listener = @listeners[channel_name] ||= Listener.new(db: @db, channel_name:, logger: @logger)
          ch = Channel.new(name: channel_name, listener:)
          listener.subscribe(ch)
          ch
        end
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
      # @option listener [Listener]
      def initialize(name: NOTIFY_CHANNEL, listener:)
        @name = name
        @running = false
        @listener = listener
        @queue = Sourced.config.executor.new_queue
      end

      def <<(message)
        @queue << message
        self
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

        @running = true

        handler ||= block

        while (msg = @queue.pop)
          handler.call(msg, self)
        end

        @running = false

        self
      end

      # Mark the channel to stop on the next tick
      def stop
        return self unless @running

        @listener.unsubscribe self
        @queue << nil
        self
      end
    end
  end
end
