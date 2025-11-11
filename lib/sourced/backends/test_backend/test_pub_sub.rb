# frozen_string_literal: true

module Sourced
  module Backends
    class TestBackend
      # An in-memory pubsub implementation for testing
      class TestPubSub
        def initialize
          @channels = {}
        end

        # @param channel_name [String]
        # @return [Channel]
        def subscribe(channel_name)
          @channels[channel_name] ||= Channel.new(channel_name)
        end

        # @param channel_name [String]
        # @param event [Sourced::Message]
        # @return [self]
        def publish(channel_name, event)
          channel = @channels[channel_name]
          channel&.publish(event)
          self
        end

        class Channel
          attr_reader :name

          def initialize(name)
            @name = name
            @handlers = []
          end

          def start(handler: nil, &block)
            handler ||= block
            @handlers << handler
          end

          def publish(event)
            @handlers.each do |handler|
              catch(:stop) do
                handler.call(event, self)
              end
            end
          end

          def stop = nil
        end
      end

    end
  end
end
