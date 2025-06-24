# frozen_string_literal: true

require 'socket'
require 'thread'
require 'json'
require 'fileutils'

module Sourced
  module Backends
    # A PubSub implementation using Unix domain sockets for inter-process communication
    # on the same machine. Each channel creates its own socket file.
    #
    # @example
    #
    #   # Publisher
    #   pubsub = Sourced::Backends::SocketPubSub.new(socket_dir: '/tmp/sourced_sockets')
    #   pubsub.publish('my_channel', event)
    #
    #   # Subscriber
    #   channel = pubsub.subscribe('my_channel')
    #   channel.start do |event, ch|
    #     case event
    #     when MyEvent
    #       # do something
    #     end
    #   end
    #
    class SocketPubSub
      DEFAULT_SOCKET_DIR = '/tmp/sourced_sockets'

      # @param socket_dir [String] Directory to store socket files
      def initialize(socket_dir: DEFAULT_SOCKET_DIR)
        @socket_dir = socket_dir
        FileUtils.mkdir_p(@socket_dir)
      end

      # @param channel_name [String]
      # @return [Channel]
      def subscribe(channel_name)
        Channel.new(socket_dir: @socket_dir, name: channel_name)
      end

      # @param channel_name [String]
      # @param event [Sourced::Message]
      # @return [self]
      def publish(channel_name, event)
        socket_path = File.join(@socket_dir, "#{channel_name}.sock")
        
        # Only publish if there are subscribers (socket exists)
        return self unless File.exist?(socket_path)

        begin
          UNIXSocket.open(socket_path) do |socket|
            event_data = JSON.dump(event.to_h)
            socket.write("#{event_data}\n")
          end
        rescue Errno::ECONNREFUSED, Errno::ENOENT
          # Socket exists but no one is listening, or socket was removed
          # This is expected behavior - messages are dropped if no subscribers
        end

        self
      end

      private

      attr_reader :socket_dir
    end

    class SocketChannel
      attr_reader :name, :socket_path

      # @param socket_dir [String] Directory containing socket files
      # @param name [String] Channel name
      def initialize(socket_dir:, name:)
        @socket_dir = socket_dir
        @name = name
        @socket_path = File.join(socket_dir, "#{name}.sock")
        @running = false
        @server = nil
        @mutex = Mutex.new
      end

      # Start listening to incoming events via Unix socket
      #
      # @param handler [#call, nil] a callable object to use as an event handler
      # @yieldparam [Sourced::Message] The received event
      # @yieldparam [SocketChannel] This channel instance
      # @return [self]
      def start(handler: nil, &block)
        @mutex.synchronize do
          return self if @running

          handler ||= block
          @running = true

          # Clean up any existing socket file
          File.unlink(@socket_path) if File.exist?(@socket_path)

          @server = UNIXServer.new(@socket_path)
          
          # Start accepting connections in a separate thread
          @listener_thread = Thread.new do
            begin
              while @running
                begin
                  # Accept connections with a timeout to check @running periodically
                  if IO.select([@server], nil, nil, 1)
                    client = @server.accept
                    handle_client(client, handler)
                  end
                rescue => e
                  # Log error but continue listening
                  puts "Error in socket listener: #{e.message}" if $DEBUG
                end
              end
            ensure
              cleanup
            end
          end
        end

        self
      end

      # Stop listening and clean up resources
      def stop
        @mutex.synchronize do
          @running = false
          @listener_thread&.join(5) # Wait up to 5 seconds for thread to finish
          cleanup
        end
      end

      # Public so that we can test this separately from async channel listening
      def parse(payload)
        data = JSON.parse(payload.strip, symbolize_names: true)
        Sourced::Message.from(data)
      end

      private

      def handle_client(client, handler)
        begin
          while @running && (line = client.gets)
            event = parse(line)
            handler.call(event, self)
          end
        rescue => e
          puts "Error handling client: #{e.message}" if $DEBUG
        ensure
          client.close rescue nil
        end
      end

      def cleanup
        @server&.close rescue nil
        File.unlink(@socket_path) if File.exist?(@socket_path)
        @server = nil
        @listener_thread = nil
      end
    end

    # Alias for backwards compatibility
    Channel = SocketChannel
  end
end