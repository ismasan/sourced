# frozen_string_literal: true

require 'sequel'
require 'thread'
require 'json'

module Sourced
  module PubSub
    # a PubSub implementation using Postgres' LISTEN/NOTIFY
    #
    # This class provides a publish-subscribe mechanism using Postgres LISTEN/NOTIFY,
    # with connection pooling - each channel name reuses a single database
    # listener per process, allowing multiple subscribers to share the same listener thread / fiber.
    # Relies on Sourced's configured Executor to use Threads or Fibers for concurrency
    #
    # @example Publishing a message
    #   pubsub = Sourced.config.pubsub
    #   event = MyEvent.new(stream_id: '111', payload: 'hello')
    #   pubsub.publish('my_channel', event)
    #
    # @example Subscribing to messages
    #   pubsub = Sourced.config.pubsub
    #   channel = pubsub.subscribe('my_channel')
    #   channel.start do |event, ch|
    #     case event
    #     when MyEvent
    #       puts "Received: #{event}"
    #     end
    #   end
    #
    # @example Multiple subscribers to the same channel
    #   # Both threads listen to the same channel but receive all messages
    #   Thread.new do
    #     ch1 = pubsub.subscribe('events')
    #     ch1.start { |event| puts "Subscriber 1: #{event}" }
    #   end
    #
    #   Thread.new do
    #     ch2 = pubsub.subscribe('events')
    #     ch2.start { |event| puts "Subscriber 2: #{event}" }
    #   end
    #
    class PG
      # Listener holds a single DB connection per channel name/process
      # Channel instances with the same name instantiated in different threads/fibers but the same process
      # share a Listener instance
      # The Listener receives messages and dispatches them to all channels.
      class Listener
        # @param db [Sequel::Database] the database connection for listening
        # @param channel_name [String] the name of the Postgres channel to listen on
        # @param timeout [Integer] the timeout in seconds for listen operations (default: 2)
        # @param logger [Logger]
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

        # Subscribe a channel to this listener
        #
        # @param channel [Channel] the channel to subscribe
        # @return [self]
        def subscribe(channel)
          start
          @queue << [:subscribe, channel]
          self
        end

        # Unsubscribe a channel from this listener
        #
        # @param channel [Channel] the channel to unsubscribe
        # @return [self]
        def unsubscribe(channel)
          @queue << [:unsubscribe, channel]
          self
        end

        # Start the listener threads for database listening and message dispatching
        #
        # @return [self]
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
                    @logger.info { "#{@info} unsubscribe channel #{channel.object_id}" }
                    if @channels.empty?
                      @logger.info { "#{@info} all channels unsubscribed." }
                    end
                  end
                in [:subscribe, channel]
                  @logger.info { "#{@info} subscribe channel #{channel.object_id}" }
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

          @logger.info { "#{@info} Started" }
        end

        # Stop the listener threads and unsubscribe all channels
        #
        # @return [self]
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

      # Initialize a new PubSub instance with database and logger
      #
      # @param db [Sequel::Database] the database connection for publishing and listening
      # @param logger [Logger] the logger instance for recording events
      def initialize(db:, logger:)
        @db = db
        @logger = logger
        @mutex = Mutex.new
        @listeners ||= {}
      end

      # Subscribe to messages on a channel
      #
      # Creates or reuses a listener for the given channel name and returns a new channel object
      # that can be used to receive messages. Multiple subscribers to the same channel share a
      # single database listener per process.
      #
      # @param channel_name [String] the name of the channel to subscribe to
      # @return [Channel] a new channel object for receiving messages
      def subscribe(channel_name)
        @mutex.synchronize do
          listener = @listeners[channel_name] ||= Listener.new(db: @db, channel_name:, logger: @logger)
          ch = Channel.new(name: channel_name, listener:)
          listener.subscribe(ch)
          ch
        end
      end

      # Publish a message to a channel
      #
      # Sends an event to all subscribers on the given channel via Postgres NOTIFY.
      #
      # @param channel_name [String] the name of the channel to publish to
      # @param event [Sourced::Message] the message to publish
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

      # Initialize a new channel for receiving messages
      #
      # @param name [String] the name of the channel (default: NOTIFY_CHANNEL)
      # @param listener [Listener] the listener managing this channel
      def initialize(name: NOTIFY_CHANNEL, listener:)
        @name = name
        @running = false
        @listener = listener
        @queue = Sourced.config.executor.new_queue
      end

      # Add a message to this channel's queue
      #
      # @param message [Sourced::Message] the message to queue
      # @return [self]
      def <<(message)
        @queue << message
        self
      end

      # Start listening to incoming events on this channel
      #
      # Blocks until the channel is stopped. Messages are passed to either the provided
      # handler or the given block.
      #
      # @param handler [#call, nil] a callable object to use as an event handler
      # @yieldparam message [Sourced::Message] the incoming message
      # @yieldparam channel [Channel] this channel instance
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

      # Stop listening on this channel
      #
      # Unsubscribes from the listener and marks the channel to stop processing messages.
      # The stop takes effect on the next iteration of the message loop.
      #
      # @return [self]
      def stop
        return self unless @running

        @listener.unsubscribe self
        @queue << nil
        self
      end
    end
  end
end
