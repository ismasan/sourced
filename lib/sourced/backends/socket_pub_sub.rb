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
        channel_dir = File.join(@socket_dir, channel_name)
        return self unless Dir.exist?(channel_dir)

        event_data = JSON.dump(event.to_h)
        
        # Find all subscriber socket files for this channel
        subscriber_sockets = Dir.glob(File.join(channel_dir, "*.sock"))
        
        subscriber_sockets.each do |socket_path|
          begin
            UNIXSocket.open(socket_path) do |socket|
              socket.write("#{event_data}\n")
            end
          rescue Errno::ECONNREFUSED, Errno::ENOENT
            # Subscriber socket exists but no one is listening
            # Clean up stale socket files
            File.unlink(socket_path) rescue nil
          end
        end

        self
      end

      private

      attr_reader :socket_dir

      class Channel
        attr_reader :name, :socket_path

        # @param socket_dir [String] Directory containing socket files
        # @param name [String] Channel name
        def initialize(socket_dir:, name:)
          @socket_dir = socket_dir
          @name = name
          @channel_dir = File.join(socket_dir, name)
          
          # Create unique socket file for this subscriber
          # Using process ID, thread ID, and timestamp to ensure uniqueness
          subscriber_id = "#{Process.pid}_#{Thread.current.object_id}_#{Time.now.to_f}"
          @socket_path = File.join(@channel_dir, "#{subscriber_id}.sock")
          
          @running = false
          @server = nil
          @listener_thread = nil
          @mutex = Mutex.new
          @handler = nil
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

            @handler = handler || block
            @running = true

            # Create channel directory if it doesn't exist
            FileUtils.mkdir_p(@channel_dir)

            # Create unique socket file for this subscriber
            @server = UNIXServer.new(@socket_path)
            
            # Start accepting connections in a separate thread
            @listener_thread = Thread.new do
              begin
                while @running
                  begin
                    # Accept connections with a timeout to check @running periodically
                    if IO.select([@server], nil, nil, 1)
                      client = @server.accept
                      handle_client(client)
                    end
                  rescue => e
                    puts "Error in socket listener: #{e.message}" if $DEBUG
                  end
                end
              ensure
                cleanup
              end
            end
          end

          @listener_thread.join
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

        def handle_client(client)
          begin
            while @running && (line = client.gets)
              event = parse(line)
              @handler.call(event, self)
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
    end
  end
end
