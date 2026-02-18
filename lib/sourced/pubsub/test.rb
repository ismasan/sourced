# frozen_string_literal: true

module Sourced
  module PubSub
    # An in-memory pubsub implementation for testing and development.
    # Thread/fiber-safe. Each subscriber gets its own queue,
    # so a slow consumer won't block other subscribers.
    class Test
      def initialize
        @mutex = Mutex.new
        @subscribers = Hash.new { |h, k| h[k] = [] }
      end

      # @param channel_name [String]
      # @return [Channel]
      def subscribe(channel_name)
        channel = Channel.new(name: channel_name, pubsub: self)
        @mutex.synchronize do
          @subscribers[channel_name] << channel
        end
        channel
      end

      # Remove a channel from the subscriber list.
      # @param channel [Channel]
      def unsubscribe(channel)
        @mutex.synchronize do
          @subscribers[channel.name]&.delete(channel)
        end
      end

      # @param channel_name [String]
      # @param event [Sourced::Message]
      # @return [self]
      def publish(channel_name, event)
        channels = @mutex.synchronize { @subscribers[channel_name].dup }
        channels.each { |ch| ch << event }
        self
      end

      class Channel
        attr_reader :name

        def initialize(name:, pubsub:)
          @name = name
          @pubsub = pubsub
          @queue = Sourced.config.executor.new_queue
        end

        # Push a message into this channel's queue.
        # @param message [Sourced::Message]
        # @return [self]
        def <<(message)
          @queue << message
          self
        end

        # Block and process messages from the queue.
        # @param handler [#call, nil]
        # @yieldparam message [Sourced::Message]
        # @yieldparam channel [Channel]
        # @return [self]
        def start(handler: nil, &block)
          handler ||= block

          while (msg = @queue.pop)
            handler.call(msg, self)
          end

          self
        end

        # Stop processing and unsubscribe from the pubsub.
        # @return [self]
        def stop
          @pubsub.unsubscribe(self)
          @queue << nil
          self
        end
      end
    end
  end
end
