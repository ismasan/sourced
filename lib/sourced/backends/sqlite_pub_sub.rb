# frozen_string_literal: true

require 'json'
require 'thread'

module Sourced
  module Backends
    # SQLite-based pub/sub implementation that works across processes
    # Uses a SQLite table to store transient messages with polling-based delivery
    class SqlitePubSub
      # Default polling interval when no messages are found
      DEFAULT_POLL_INTERVAL = 0.1
      # Maximum polling interval (with exponential backoff)
      MAX_POLL_INTERVAL = 0.5
      # Message retention time (clean up after 10 seconds)
      MESSAGE_RETENTION = 10

      class DefaultSerializer
        def serialize(event) = JSON.dump(event.to_h)

        def deserialize(payload)
          data = JSON.parse(payload, symbolize_names: true)
          Sourced::Message.from(data)
        end
      end

      class JSONSerializer
        def serialize(event) = JSON.dump(event.to_h)

        def deserialize(payload)
          JSON.parse(payload, symbolize_names: true)
        end
      end

      def initialize(db:, serializer: DefaultSerializer.new, table_name: 'sourced_pubsub_messages')
        @db = db
        @table_name = table_name.to_sym
        @channels = {}
        @cleanup_thread = nil
        @serializer = serializer
        @mutex = Mutex.new
        ensure_table_exists
        start_cleanup_thread
      end

      # Subscribe to a channel
      # @param channel_name [String]
      # @return [Channel]
      def subscribe(channel_name)
        # Create a new channel instance for each subscriber to avoid shared state
        Channel.new(channel_name, @db, @table_name, @serializer)
      end

      # Publish a message to a channel
      # @param channel_name [String]
      # @param event [Sourced::Message]
      # @return [self]
      def publish(channel_name, event)
        now = Time.now
        payload = @serializer.serialize(event)
        @db[@table_name].insert(
          channel_name: channel_name,
          payload: payload,
          created_at: now,
          expires_at: now + MESSAGE_RETENTION
        )
        self
      end

      # Cleanup method for testing
      def cleanup
        stop_cleanup_thread
        @db.drop_table?(@table_name)
        @channels.clear
      end

      private

      def ensure_table_exists
        return if @db.table_exists?(@table_name)

        puts "Creating pubsub table #{@table_name}" if $DEBUG
        @db.create_table(@table_name) do
          primary_key :id
          String :channel_name, null: false
          Text :payload, null: false
          Time :created_at, null: false
          Time :expires_at, null: false
          
          index :channel_name
          index :created_at
          index :expires_at
        end
        puts "Created pubsub table #{@table_name}" if $DEBUG
      end

      def start_cleanup_thread
        # TODO: use the configured executor
        @cleanup_thread = Thread.new do
          loop do
            begin
              sleep(60) # Run cleanup every minute
              cleanup_expired_messages
            rescue StandardError => e
              # Log error but keep thread alive
              warn "SqlitePubSub cleanup error: #{e.message}"
            end
          end
        rescue StandardError => e
          warn "SqlitePubSub cleanup thread died: #{e.message}"
        end
      end

      def stop_cleanup_thread
        if @cleanup_thread
          @cleanup_thread.kill
          @cleanup_thread.join(1)
          @cleanup_thread = nil
        end
      end

      def cleanup_expired_messages
        deleted = @db[@table_name].where(Sequel[:expires_at] < Time.now).delete
        puts "Cleaned up #{deleted} expired pubsub messages" if deleted > 0
      end

      class Channel
        attr_reader :name

        def initialize(name, db, table_name, serializer)
          @name = name
          @db = db
          @table_name = table_name
          @handlers = []
          @last_id = 0
          @stop_requested = false
          @serializer = serializer
          @started_at = Time.now
        end

        # Start listening for messages
        # @param handler [Proc] optional handler instead of block
        # @yield [event, channel] block to handle messages
        def start(handler: nil, &block)
          handler ||= block
          return unless handler

          @handlers << handler
          catch(:stop) do
            start_blocking_poll(handler)
          end
        end

        # Stop the channel
        def stop
          @stop_requested = true
        end


        private

        def start_blocking_poll(handler)
          poll_interval = DEFAULT_POLL_INTERVAL
          
          loop do
            break if @stop_requested

            begin
              messages = fetch_new_messages
              
              if messages.any?
                messages.each do |msg|
                  event = @serializer.deserialize(msg[:payload])
                  handler.call(event, self)
                  @last_id = msg[:id]
                end
                
                # Reset poll interval when we found messages
                poll_interval = DEFAULT_POLL_INTERVAL
              # else
              #   # Exponential backoff when no messages
              #   poll_interval = [poll_interval * 1.5, MAX_POLL_INTERVAL].min
              end

              sleep(poll_interval) unless @stop_requested
            rescue StandardError => e
              warn "Error in pubsub polling: #{e.message}"
              sleep(poll_interval)
            end
          end
        rescue StandardError => e
          warn "Pubsub polling died: #{e.message}"
        end

        def fetch_new_messages
          # Only fetch messages that were created after this subscriber started,
          # or after @last_id if we've already processed some messages
          cutoff_time = @started_at - 1 # Allow 1 second buffer for messages just before we started
          
          messages = @db[@table_name]
            .where(channel_name: @name)
            .where(Sequel[:id] > @last_id)
            .where(Sequel[:created_at] > cutoff_time)
            .where(Sequel[:expires_at] > Time.now)
            .order(:id)
            .limit(100) # Process in batches
            .all
          puts "Fetched #{messages.size} messages for channel #{@name}, last_id: #{@last_id}, started_at: #{@started_at}" if $DEBUG && messages.any?
          messages
        end
      end
    end
  end
end
