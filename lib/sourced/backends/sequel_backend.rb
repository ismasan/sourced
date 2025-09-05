# frozen_string_literal: true

require 'sequel'
require 'json'
require 'sourced/message'
require 'sourced/backends/sequel_pub_sub'

Sequel.extension :pg_json if defined?(PG)

module Sourced
  module Backends
    # Production backend implementation using Sequel ORM for PostgreSQL and SQLite.
    # This backend provides persistent storage for events, commands, and consumer state
    # with support for concurrency control, pub/sub notifications, and transactional processing.
    #
    # The SequelBackend handles:
    # - Event storage and retrieval with ordering guarantees
    # - Command scheduling and dispatching
    # - Consumer group management and offset tracking  
    # - Concurrency control via database locks
    # - Pub/sub notifications for real-time event processing
    #
    # @example Basic setup with PostgreSQL
    #   db = Sequel.connect('postgres://localhost/myapp')
    #   backend = Sourced::Backends::SequelBackend.new(db)
    #   backend.install unless backend.installed?
    #
    # @example Configuration in Sourced
    #   Sourced.configure do |config|
    #     config.backend = Sequel.connect(ENV['DATABASE_URL'])
    #   end
    class SequelBackend
      # Consumer group status indicating active processing
      ACTIVE = 'active'
      # Consumer group status indicating stopped processing
      STOPPED = 'stopped'

      # @!attribute [r] pubsub
      #   @return [SequelPubSub] Pub/sub implementation for real-time notifications
      attr_reader :pubsub

      # Initialize a new Sequel backend instance.
      # Automatically sets up database connection, table names, and pub/sub system.
      #
      # @param db [Sequel::Database, String] Database connection or connection string
      # @param logger [Object] Logger instance for backend operations (defaults to configured logger)
      # @param prefix [String] Table name prefix for Sourced tables (defaults to 'sourced')
      # @example Initialize with existing connection
      #   backend = SequelBackend.new(Sequel.connect('postgres://localhost/mydb'))
      # @example Initialize with custom prefix
      #   backend = SequelBackend.new(db, prefix: 'my_app')
      def initialize(db, logger: Sourced.config.logger, prefix: 'sourced')
        @db = connect(db)
        @pubsub = SequelPubSub.new(db: @db)
        @logger = logger
        @prefix = prefix
        @commands_table = table_name(:commands)
        @scheduled_messages_table = table_name(:scheduled_messages)
        @streams_table = table_name(:streams)
        @offsets_table = table_name(:offsets)
        @consumer_groups_table = table_name(:consumer_groups)
        @events_table = table_name(:events)
        @setup = false
        logger.info("Connected to #{@db}")
      end

      # Schedule commands for background processing by a specific consumer group.
      # Commands are serialized and stored in the commands table for later dispatch.
      #
      # @param commands [Array<Command>] Commands to schedule for processing
      # @param group_id [String] Consumer group ID that will process these commands
      # @return [Boolean] true if commands were scheduled, false if array was empty
      # @example Schedule commands for processing
      #   commands = [CreateCart.new(stream_id: 'cart-1', payload: {})]
      #   backend.schedule_commands(commands, group_id: 'CartActor')
      def schedule_commands(commands, group_id:)
        return false if commands.empty?

        rows = commands.map { |c| serialize_command(c, group_id:) }

        db.transaction do
          # TODO: reactors will be created in advance
          # Here we can just update it.
          upsert_consumer_group(group_id)
          db[commands_table].multi_insert(rows)
        end
        true
      end

      # TODO: if the application raises an exception
      # the command row is not deleted, so that it can be retried.
      # However, if a command fails _permanently_ there's no point in keeping it in the queue,
      # this ties with unresolved error handling in event handling, too.
      #
      # @yield [command] Optional block to reserve the command for processing
      # @yieldparam command [Command] The command to potentially reserve
      # @yieldreturn [Boolean] true to delete the command (reserve it), false to keep it
      # @return [Command, nil] The next command or nil if no commands available
      # @example Get next command without reserving
      #   command = backend.next_command
      # @example Reserve command for processing
      #   backend.next_command do |command|
      #     # Process command and return true to remove from queue
      #     process_command(command)
      #     true
      #   end
      # @note Commands that fail processing are kept in the queue for retry
      def next_command(&reserve)
        now = Time.now

        if block_given?
          db.transaction do
            row = db.fetch(sql_for_next_command, now, now).first
            return unless row

            cmd = deserialize_event(row)
            # reserve block can return falsey
            # to keep the command in the bus. For example when retrying.
            if yield(cmd)
              db[commands_table].where(id: cmd.id).delete
            end
            cmd
          end
        else
          db.transaction do
            row = db.fetch(sql_for_next_command, now, now).first
            row ? deserialize_event(row) : nil
          end
        end
      end

      # Data structure for backend statistics and monitoring information.
      # @!attribute [r] stream_count
      #   @return [Integer] Total number of event streams
      # @!attribute [r] max_global_seq
      #   @return [Integer] Highest global sequence number (latest event)
      # @!attribute [r] groups
      #   @return [Array<Hash>] Consumer group information with processing stats
      Stats = Data.define(:stream_count, :max_global_seq, :groups)

      # Get comprehensive statistics about the backend state.
      # Useful for monitoring, debugging, and understanding system health.
      #
      # @return [Stats] Statistics object with stream counts, sequences, and group info
      # @example Get backend statistics
      #   stats = backend.stats
      #   puts "Total streams: #{stats.stream_count}"
      #   puts "Latest event: #{stats.max_global_seq}"
      #   stats.groups.each { |g| puts "Group #{g[:id]}: #{g[:status]}" }
      def stats
        stream_count = db[streams_table].count
        max_global_seq = db[events_table].max(:global_seq)
        groups = db.fetch(sql_for_consumer_stats).all
        Stats.new(stream_count, max_global_seq, groups)
      end

      # Data structure representing a stream with its metadata.
      # @!attribute [r] stream_id
      #   @return [String] Unique identifier for the stream
      # @!attribute [r] seq
      #   @return [Integer] Latest sequence number in the stream
      # @!attribute [r] updated_at
      #   @return [Time] Timestamp of the most recent event in the stream
      Stream = Data.define(:stream_id, :seq, :updated_at)

      # Retrieve a list of recently active streams, ordered by most recent activity.
      # This method is useful for diagnostics, monitoring, and debugging to understand
      # which streams have been most active in the system.
      #
      # The query is optimized with a database index on updated_at for efficient sorting
      # and uses LIMIT to minimize data transfer.
      #
      # @param limit [Integer] Maximum number of streams to return (defaults to 10, must be >= 0)
      # @return [Array<Stream>] Array of Stream objects ordered by updated_at descending
      # @raise [ArgumentError] if limit is negative
      # @example Get the 5 most recently active streams
      #   recent = backend.recent_streams(limit: 5)
      #   recent.each do |stream|
      #     puts "Stream #{stream.stream_id}: seq #{stream.seq} at #{stream.updated_at}"
      #   end
      # @example Monitor system activity
      #   streams = backend.recent_streams(limit: 20)
      #   active_count = streams.count { |s| s.updated_at > 1.hour.ago }
      #   puts "#{active_count} streams active in last hour"
      def recent_streams(limit: 10)
        # Input validation
        return [] if limit == 0
        raise ArgumentError, "limit must be a positive integer" if limit < 0

        # Optimized query with index on updated_at
        query = db[streams_table]
          .select(:stream_id, :seq, :updated_at)
          .reverse(:updated_at)  # More idiomatic Sequel than order(Sequel.desc(:updated_at))
          .limit(limit)

        query.map do |row|
          Stream.new(**row)
        end
      end

      def transaction(&)
        db.transaction(&)
      end

      def schedule_messages(messages, at:)
        return false if messages.empty?

        now = Time.now
        rows = messages.map do |m|
          message_data = m.to_h
          message_data[:metadata] = message_data[:metadata].merge(scheduled_at: now)
          { created_at: now, available_at: at, message: JSON.dump(message_data) }
        end

        transaction do
          db[scheduled_messages_table].multi_insert(rows)
        end

        true
      end

      def update_schedule!
        now = Time.now

        transaction do
          rows = db[scheduled_messages_table]
            .where { available_at <= now }
            .order(:id)
            .limit(100)
            .for_update
            .skip_locked
            .all

          return 0 if rows.empty?

          # Process all messages
          messages = rows.map do |r|
            data = JSON.parse(r[:message], symbolize_names: true)
            data[:created_at] = now
            Message.from(data)
          end

          # TODO: appending one-by-one is very bad
          # Fix #append_next_to_stream to support multiple messages
          messages.each do |m|
            append_next_to_stream(m.stream_id, m)
          end

          # Batch delete all processed messages
          row_ids = rows.map { |m| m[:id] }
          db[scheduled_messages_table].where(id: row_ids).delete

          rows.size
        end
      end

      # Append a single event to a stream, creating the stream if it doesn't exist.
      # Also automatically increments the sequence number for the stream and event,
      # without relying on the client passing the right sequence number.
      # This is used for appending events in order without client-side optimistic concurrency control.
      # For example commands that will be picked up and handled by a reactor, later.
      # @param stream_id [String] Unique identifier for the event stream
      # @param event [Sourced::Message] Event to append to the stream
      # @option max_retries [Integer] Maximum number of retries on unique constraint violation (default: 3)
      def append_next_to_stream(stream_id, event, max_retries: 3)
        retries = 0
        begin
          db.transaction do
            # Update or create a stream.
            # If updating, increment seq by 1.
            stream_record = db[streams_table]
              .insert_conflict(
                target: :stream_id, 
                update: { 
                  seq: Sequel.qualify(streams_table, :seq) + 1,
                  updated_at: Time.now 
                }
              )
              .returning(:id, :seq)
              .insert(stream_id:, seq: 1)
              .first

            # Insert the event into the events table, with the (possibly incremented) seq.
            row = serialize_event(event, stream_record[:id])
            row[:seq] = stream_record[:seq]
            db[events_table].insert(row)
          end

          true
        rescue Sequel::UniqueConstraintViolation => e
          retries += 1
          if retries <= max_retries
            sleep(0.001 * retries) # Brief backoff
            retry
          else
            raise Sourced::ConcurrentAppendError, e.message
          end
        end
      end

      def append_to_stream(stream_id, events)
        return if events.empty?

        if events.map(&:stream_id).uniq.size > 1
          raise ArgumentError, 'Events must all belong to the same stream'
        end

        db.transaction do
          seq = events.last.seq
          id = db[streams_table].insert_conflict(target: :stream_id, update: { seq:, updated_at: Time.now }).insert(stream_id:, seq:)
          rows = events.map { |e| serialize_event(e, id) }
          db[events_table].multi_insert(rows)
        end

        true
      rescue Sequel::UniqueConstraintViolation => e
        raise Sourced::ConcurrentAppendError, e.message
      end

      # Reserve next event for a reactor, based on the reactor's #handled_messages list
      # This fetches the next un-acknowledged event for the reactor, and processes it in a transaction
      # which aquires a lock on the event's stream_id and the reactor's group_id
      # So that no other reactor instance in the same group can process the same stream concurrently.
      # This way, events for the same stream are guaranteed to be processed sequentially for each reactor group.
      # If the given block returns truthy, the event is marked as acknowledged for this reactor group
      # (ie. it won't be processed again by the same group).
      # If the block returns falsey, the event is NOT acknowledged and will be retried,
      # unless the block also stops the reactor, which is the default behaviour when an exception is raised.
      # See Router#handle_next_event_for_reactor,
      # A boolean is passed to the block to indicate whether the event is being replayed 
      # (ie the reactor group has previously processed it).
      # This is done by incrementing the group's highest_global_seq with every event acknowledged.
      # When the group's offsets are reset (in order to re-process all events), this value is preserved
      # so that each event's global_seq can be compared against it to determine if the event is being replayed.
      #
      # @example
      #   backend.reserve_next_for_reactor(reactor) do |event, replaying|
      #     # process event here
      #     true # ACK event
      #   end
      #
      # @param reactor [Sourced::ReactorInterface]
      # @option worker_id [String]
      # @yieldparam [Sourced::Message]
      # @yieldparam [Boolean] whether the event is being replayed (ie. it has been processed before)
      # @yieldreturn [Boolean] whether to ACK the event for this reactor group and stream ID.
      def reserve_next_for_reactor(reactor, worker_id: nil, &)
        worker_id ||= [Process.pid, Thread.current.object_id, Fiber.current.object_id].join('-')
        group_id = reactor.consumer_info.group_id
        handled_messages = reactor.handled_messages.map(&:type).uniq
        now = Time.now

        bootstrap_offsets_for(group_id)

        # Phase 1: Claim stream_id/group_id in short transaction
        # This claim includes :event, :claim_id and :replaying
        row = claim_next_event(group_id, handled_messages, now, reactor.consumer_info.start_from.call, worker_id)

        # return reserve_next_for_reactor(reactor, &) if row == :retry
        return unless row

        event = deserialize_event(row)

        begin
          # Phase 2: Process event outside of transaction
          action = yield(event, row[:replaying])

          # Phase 3: update state depending on result
          # Result handling happens in the same transaction
          # as ACKing the event
          db.transaction do
            process_action(group_id, row, action, event)
          end

        rescue StandardError
          release_offset(row[:offset_id])
          raise
        end

        event
      end

      private def release_offset(offset_id)
        db[offsets_table].where(id: offset_id).update(claimed: false, claimed_at: nil, claimed_by: nil)
      end

      private def process_action(group_id, row, action, event)
        case action
        when Actions::OK
          ack_event(group_id, row[:stream_id_fk], row[:global_seq])

        when Actions::AppendNext
          messages = action.messages.map do |msg|
            event.correlate(msg)
          end
          messages.each do |msg|
            append_next_to_stream(msg.stream_id, msg)
          end
          ack_event(group_id, row[:stream_id_fk], row[:global_seq])

        when Actions::AppendAfter
          messages = action.messages.map do |msg|
            event.correlate(msg)
          end
          append_to_stream(action.stream_id, messages)
          ack_event(group_id, row[:stream_id_fk], row[:global_seq])

        when Actions::Schedule
          messages = action.messages.map do |msg|
            event.correlate(msg)
          end
          schedule_messages(messages, at: action.at)
          ack_event(group_id, row[:stream_id_fk], row[:global_seq])

        when Actions::RETRY
          release_offset(row[:offset_id])

        else
          raise ArgumentError, "Unexpected Sourced::Actions type, but got: #{action.class}. Group #{group_id}. Message #{event}"
        end
      end

      # Insert missing offsets for a consumer group and all streams.
      # This runs in advance of claiming events for processing
      # so that the claim query relies on existing offsets and FOR UPDATE SKIP LOCKED
      # to prevent concurrent claims on the same stream by different workers.
      private def bootstrap_offsets_for(group_id)
        now = Time.now
        db.transaction do
          group = db[consumer_groups_table].where(group_id:, status: ACTIVE).first
          return if group.nil?

          # Find streams that don't have offsets for this group
          missing_streams = db[streams_table]
            .left_join(offsets_table,
              stream_id: :id, 
              group_id: group[:id])
            .where(Sequel[offsets_table][:id] => nil)
            .select(Sequel[streams_table][:id], Sequel[streams_table][:stream_id])
            .limit(100)

          # Prepare offset records for bulk insert
          offset_records = missing_streams.map do |stream|
            {
              group_id: group[:id],
              stream_id: stream[:id],
              global_seq: 0,
              claimed: false,
              created_at: now
            }
          end

          if offset_records.any?
            db[offsets_table].insert_conflict.multi_insert(offset_records)
          end
          
          offset_records.size
        end
      end

      # @param group_id [String] Consumer group ID to claim the next event for
      # @param handle_messages [Array<String>] List of message types to handle
      # @param now [Time] Current time
      # @param start_from [Time] Optional starting point for event processing
      # @param worker_id [String] Unique identifier for the worker claiming the event
      private def claim_next_event(group_id, handled_messages, now, start_from, worker_id)
        db.transaction do
          # 1. get next event for this group_id and handled_messages
          # Use FOR UPDATE SKIP LOCKED if supported
          row = if start_from.is_a?(Time)
            db.fetch(sql_for_reserve_next_with_events(handled_messages, group_id:, now:, start_from:)).first
          else
            db.fetch(sql_for_reserve_next_with_events(handled_messages, group_id:, now:)).first
          end
          return unless row

          # An offset exists, and has been locked with FOR UPDATE SKIP LOCKED
          # while in the same transaction, we claim it
          updated = db[offsets_table]
            .where(id: row[:offset_id])
            .update(claimed: true, claimed_at: now, claimed_by: worker_id)

          return nil if updated == 0 # already claimed by another worker
          row
        end

      rescue Sequel::UniqueConstraintViolation
        # Another worker has already claimed this event
        logger.debug "AAA Claim for group #{group_id} already exists, skipping"
        nil
      end

      def register_consumer_group(group_id)
        db[consumer_groups_table]
          .insert_conflict(target: :group_id, update: nil)
          .insert(group_id:, status: ACTIVE)
      end

      # Fetch and update a consumer group in a transaction
      # Used by Router to stop / retry consumer groups
      # when handing exceptions.
      #
      # @example
      #   backend.updating_consumer_group(group_id) do |group|
      #     group.stop('some reason')
      #   end
      #
      # @param group_id [String]
      # @yield [GroupUpdater]
      def updating_consumer_group(group_id, &)
        dataset = db[consumer_groups_table].where(group_id:)
        group_row = dataset.for_update.first
        raise ArgumentError, "Consumer group #{group_id} not found" unless group_row

        ctx = group_row[:error_context] ? parse_json(group_row[:error_context]) : {}
        group_row[:error_context] = ctx
        group = GroupUpdater.new(group_id, group_row, logger)
        yield group
        updates = group.updates
        updates[:error_context] = JSON.dump(updates[:error_context])
        dataset.update(updates)
      end

      # Start a consumer group that has been stopped.
      #
      # @param group_id [String]
      def start_consumer_group(group_id)
        dataset = db[consumer_groups_table].where(group_id: group_id)
        dataset.update(status: ACTIVE, retry_at: nil, error_context: nil)
      end

      # @param group_id [String]
      # @param reason [#inspect, NilClass]
      def stop_consumer_group(group_id, reason = nil)
        updating_consumer_group(group_id) do |group|
          group.stop(reason)
        end
      end

      # Reset offsets for all streams tracked by a consumer group.
      # If the consumer group is active, this will make it re-process all events
      #
      # @param group_id [String]
      # @return [Boolean]
      def reset_consumer_group(group_id)
        db.transaction do
          row = db[consumer_groups_table].where(group_id:).select(:id).first
          return unless row

          id = row[:id]
          db[offsets_table].where(group_id: id).delete
        end

        true
      end

      def ack_on(group_id, event_id, &)
        db.transaction do
          row = db.fetch(sql_for_ack_on, group_id, event_id).first
          raise Sourced::ConcurrentAckError, "Stream for event #{event_id} is being concurrently processed by #{group_id}" unless row

          yield if block_given?

          ack_event(group_id, row[:stream_id_fk], row[:global_seq])
        end
      end

      private def ack_event(group_id, stream_id, global_seq)
        db.transaction do
          # We could use an upsert here, but
          # we create consumer groups on registration, or
          # after the first update.
          # So by this point, the consumer groups table 
          # will always have a record for the group_id.
          # So we assume it's there and update it directly.
          # If the group_id doesn't exist, it will be created below,
          # but this should only happen once per group.
          update_result = db[consumer_groups_table]
            .where(group_id:)
            .returning(:id)
            .update(
              updated_at: Sequel.function(:now),
              highest_global_seq: Sequel.function(
                :greatest, 
                :highest_global_seq,
                global_seq
              )
            )

          # Here we do issue a separate INSERT, but this should only happen
          # if the group record doesn't exist yet, for some reason.
          if update_result.empty?  # No rows were updated (record doesn't exist)
            # Only then INSERT
            update_result = db[consumer_groups_table]
              .returning(:id)
              .insert(group_id:, highest_global_seq: global_seq)
          end

          group_id = update_result[0][:id]

          # Upsert offset
          # NOTE: when using #reserve_next_for_reactor,
          # an offset will always exist for an event
          # but the synchronous ack_on method can be used with an event
          # for whose group and stream no offset exists yet.
          db[offsets_table]
            .insert_conflict(
              target: [:group_id, :stream_id],
              update: { claimed: false, claimed_at: nil, claimed_by: nil, global_seq: }
            )
            .insert(
              group_id:,
              stream_id:,
              global_seq:
            )
        end
      end

      private def base_events_query
        db[events_table]
          .select(
            Sequel[events_table][:id],
            Sequel[streams_table][:stream_id],
            Sequel[events_table][:seq],
            Sequel[events_table][:global_seq],
            Sequel[events_table][:type],
            Sequel[events_table][:created_at],
            Sequel[events_table][:causation_id],
            Sequel[events_table][:correlation_id],
            Sequel[events_table][:metadata],
            Sequel[events_table][:payload],
          )
          .join(streams_table, id: :stream_id)
      end

      def read_correlation_batch(event_id)
        correlation_subquery = db[events_table]
          .select(:correlation_id)
          .where(id: event_id)

        query = base_events_query
          .where(Sequel[events_table][:correlation_id] => correlation_subquery)
          .order(Sequel[events_table][:global_seq])

        query.map do |row|
          deserialize_event(row)
        end
      end

      def read_event_stream(stream_id, after: nil, upto: nil)
        _events_table = events_table # need local variable for Sequel block

        query = base_events_query.where(Sequel[streams_table][:stream_id] => stream_id)

        query = query.where { Sequel[_events_table][:seq] > after } if after
        query = query.where { Sequel[_events_table][:seq] <= upto } if upto
        query.order(Sequel[_events_table][:global_seq]).map do |row|
          deserialize_event(row)
        end
      end

      # For tests only
      def clear!
        raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'
        # Truncate and restart global_seq increment first
        db[events_table].truncate(cascade: true, only: true, restart: true)
        db[events_table].delete
        db[commands_table].delete
        db[consumer_groups_table].delete
        db[offsets_table].delete
        db[streams_table].delete
      end

      # Called after Sourced.configure
      def setup!(config)
        return if @setup
        if config.executor.is_a?(Sourced::AsyncExecutor)
          Sequel.extension :fiber_concurrency
        end
        @setup = true
      end

      # Check if all required database tables exist.
      # This verifies that the backend has been properly installed with all necessary schema.
      #
      # @return [Boolean] true if all required tables exist, false otherwise
      # @see #install
      def installed?
        installer.installed?
      end

      def uninstall
        installer.uninstall
        self
      end

      def install
        installer.install
        self
      end

      private

      attr_reader :db, :logger, :prefix, :events_table, :streams_table, :offsets_table, :consumer_groups_table, :commands_table, :scheduled_messages_table

      def installer
        @installer ||= Installer.new(
          @db,
          logger:,
          commands_table:,
          scheduled_messages_table:,
          streams_table:,
          offsets_table:,
          consumer_groups_table:,
          events_table:
        )
      end

      def sql_for_next_command
        <<~SQL
          WITH next_command AS (
              SELECT 
              c.id,
              c.stream_id,
              c.type,
              c.causation_id,
              c.correlation_id,
              c.metadata,
              c.payload,
              c.created_at,
              pg_try_advisory_xact_lock(
                  hashtext(c.consumer_group_id::text),
                  hashtext(c.stream_id::text)
              ) as lock_obtained
              FROM #{commands_table} c
              INNER JOIN #{consumer_groups_table} r ON c.consumer_group_id = r.group_id
              WHERE 
                r.status = '#{ACTIVE}'
                AND (r.retry_at IS NULL OR r.retry_at <= ?)
              AND c.created_at <= ? 
              ORDER BY c.created_at ASC
          )
          SELECT *
          FROM next_command
          WHERE lock_obtained = true
          LIMIT 1;
        SQL
      end

      def sql_for_reserve_next_with_events(handled_messages, group_id:, now:, start_from: nil)
        event_types = handled_messages.map { |e| "'#{e}'" }
        now = db.literal(now)
        group_id = db.literal(group_id)
        event_types_sql = event_types.any? ? " AND e.type IN(#{event_types.join(',')})" : ''
        time_window_sql = start_from ? " AND e.created_at > #{db.literal(start_from)}" : ''

        <<~SQL
          SELECT
              e.global_seq, e.id, ss.stream_id, e.stream_id as stream_id_fk,
              e.seq, e.type, e.created_at, e.causation_id, e.correlation_id,
              e.metadata, e.payload, so.id as offset_id, cg.id as group_id_fk,
              (e.global_seq <= cg.highest_global_seq) AS replaying
          FROM sourced_events e
          JOIN sourced_streams ss ON e.stream_id = ss.id
          JOIN sourced_consumer_groups cg ON cg.group_id = #{group_id}
          JOIN sourced_offsets so ON cg.id = so.group_id AND ss.id = so.stream_id
          WHERE e.global_seq > so.global_seq
            AND so.claimed = FALSE
            AND cg.status = 'active'
            AND (cg.retry_at IS NULL OR cg.retry_at <= #{now})
            #{event_types_sql}#{time_window_sql}
          ORDER BY e.global_seq ASC
          FOR UPDATE OF so SKIP LOCKED
          LIMIT 1;
        SQL
      end

      def sql_for_ack_on
        <<~SQL
          WITH candidate_rows AS (
            SELECT 
                e.global_seq,
                e.stream_id AS stream_id_fk, 
                pg_try_advisory_xact_lock(hashtext(?::text), hashtext(s.id::text)) as lock_obtained
            FROM #{events_table} e
            JOIN #{streams_table} s ON e.stream_id = s.id
            WHERE e.id = ?
          )
          SELECT *
          FROM candidate_rows
          WHERE lock_obtained = true
          LIMIT 1;
        SQL
      end

      def sql_for_consumer_stats
        @sql_for_consumer_stats ||= <<~SQL
          SELECT 
              r.group_id,
              r.status,
              r.retry_at,
              COALESCE(MIN(o.global_seq), 0) AS oldest_processed,
              COALESCE(MAX(o.global_seq), 0) AS newest_processed,
              COUNT(o.id) AS stream_count
          FROM #{consumer_groups_table} r
          LEFT JOIN #{offsets_table} o ON o.group_id = r.id AND o.global_seq > 0
          GROUP BY r.id, r.group_id, r.status, r.retry_at
          ORDER BY r.group_id;
        SQL
      end

      def table_name(name)
        [prefix, name].join('_').to_sym
      end

      def parse_json(json)
        return json unless json.is_a?(String)

        JSON.parse(json, symbolize_names: true)
      end

      def upsert_consumer_group(group_id, status: ACTIVE)
        db[consumer_groups_table]
          .insert_conflict(target: :group_id, update: { status:, updated_at: Time.now })
          .insert(group_id:, status:)
      end

      def serialize_command(cmd, group_id: nil)
        row = cmd.to_h.except(:seq)
        row[:consumer_group_id] = group_id if group_id
        row[:metadata] = JSON.dump(row[:metadata]) if row[:metadata]
        row[:payload] = JSON.dump(row[:payload]) if row[:payload]
        row
      end

      def serialize_event(event, stream_id)
        row = event.to_h
        row[:stream_id] = stream_id
        row[:metadata] = JSON.dump(row[:metadata]) if row[:metadata]
        row[:payload] = JSON.dump(row[:payload]) if row[:payload]
        row
      end

      def deserialize_event(row)
        row[:payload] = parse_json(row[:payload]) if row[:payload]
        row[:metadata] = parse_json(row[:metadata]) if row[:metadata]
        Message.from(row)
      end

      def connect(db)
        case db
        when Sequel::Database
          db
        when String, Hash
          Sequel.connect(db)
        else
          raise ArgumentError, "Invalid database connection: #{db.inspect}"
        end
      end
    end
  end
end

require 'sourced/backends/sequel_backend/installer'
require 'sourced/backends/sequel_backend/group_updater'
