# frozen_string_literal: true

require 'sequel'
require 'json'
require 'sourced/message'
require 'sourced/backends/pg_pub_sub'

Sequel.extension :pg_json if defined?(PG)

module Sourced
  module Backends
    # Production backend implementation using Sequel ORM for PostgreSQL and SQLite.
    # This backend provides persistent storage for messages, and consumer state
    # with support for concurrency control, pub/sub notifications, and transactional processing.
    #
    # The SequelBackend handles:
    # - Message storage and retrieval with ordering guarantees
    # - Message scheduling and dispatching
    # - Consumer group management and offset tracking  
    # - Concurrency control via database locks and claims
    # - Pub/sub notifications for real-time message processing
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
      #   @return [PubSub] Pub/sub implementation for real-time notifications
      attr_reader :pubsub

      # Initialize a new Sequel backend instance.
      # Automatically sets up database connection, table names, and pub/sub system.
      #
      # @param db [Sequel::Database, String] Database connection or connection string
      # @param logger [Object] Logger instance for backend operations (defaults to configured logger)
      # @param prefix [String] Table name prefix for Sourced tables (defaults to 'sourced')
      # @param schema [String, nil] Optional PostgreSQL schema name. When set, all tables are
      #   created and queried within this schema (e.g. "my_app".sourced_messages).
      #   The schema is created automatically on #install if it doesn't exist.
      #   Defaults to nil (uses the database's default search_path, typically "public").
      # @example Initialize with existing connection
      #   backend = SequelBackend.new(Sequel.connect('postgres://localhost/mydb'))
      # @example Initialize with custom prefix
      #   backend = SequelBackend.new(db, prefix: 'my_app')
      # @example Initialize with a PostgreSQL schema
      #   backend = SequelBackend.new(db, schema: 'my_app')
      # @example Combine schema and prefix
      #   backend = SequelBackend.new(db, schema: 'my_app', prefix: 'evt')
      #   # => tables: my_app.evt_messages, my_app.evt_streams, etc.
      def initialize(db, logger: Sourced.config.logger, prefix: 'sourced', schema: nil)
        @db = connect(db)
        @pubsub = PGPubSub.new(db: @db, logger:)
        @logger = logger
        @prefix = prefix
        @schema = schema
        @workers_table = table_name(:workers)
        @scheduled_messages_table = table_name(:scheduled_messages)
        @streams_table = table_name(:streams)
        @offsets_table = table_name(:offsets)
        @consumer_groups_table = table_name(:consumer_groups)
        @messages_table = table_name(:messages)
        # Literal SQL strings for use in raw SQL interpolation.
        # Sequel qualified identifiers don't produce valid SQL via #to_s,
        # so we pre-compute literal versions for the 3 raw SQL methods.
        @messages_table_literal = @db.literal(@messages_table)
        @streams_table_literal = @db.literal(@streams_table)
        @offsets_table_literal = @db.literal(@offsets_table)
        @consumer_groups_table_literal = @db.literal(@consumer_groups_table)
        @setup = false
        logger.info("Connected to #{@db}")
      end

      # Data structure for backend statistics and monitoring information.
      # @!attribute [r] stream_count
      #   @return [Integer] Total number of message streams
      # @!attribute [r] max_global_seq
      #   @return [Integer] Highest global sequence number (latest message)
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
      #   puts "Latest message: #{stats.max_global_seq}"
      #   stats.groups.each { |g| puts "Group #{g[:id]}: #{g[:status]}" }
      def stats
        stream_count = db[streams_table].count
        max_global_seq = db[messages_table].max(:global_seq)
        groups = db.fetch(sql_for_consumer_stats).all
        Stats.new(stream_count, max_global_seq, groups)
      end

      # Data structure representing a stream with its metadata.
      # @!attribute [r] stream_id
      #   @return [String] Unique identifier for the stream
      # @!attribute [r] seq
      #   @return [Integer] Latest sequence number in the stream
      # @!attribute [r] updated_at
      #   @return [Time] Timestamp of the most recent message in the stream
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

          # Append messages to each stream
          messages.group_by(&:stream_id).each do |stream_id, stream_messages|
            append_next_to_stream(stream_id, stream_messages)
          end

          # Batch delete all processed messages
          row_ids = rows.map { |m| m[:id] }
          db[scheduled_messages_table].where(id: row_ids).delete

          rows.size
        end
      end

      # Record heartbeats for a set of worker IDs.
      # Accepts a String or an Array<String>. Returns number of rows touched.
      def worker_heartbeat(worker_ids, at: Time.now)
        ids = Array(worker_ids).uniq
        return 0 if ids.empty?

        rows = ids.map { |id| { id:, last_seen: at } }
        db.transaction do
          db[workers_table]
            .insert_conflict(target: :id, update: { last_seen: at })
            .multi_insert(rows)
        end
        ids.size
      end

      # Release stale claims for workers that have not heartbeated within ttl_seconds.
      # Returns the number of offsets rows updated.
      def release_stale_claims(ttl_seconds: 120)
        cutoff = Time.now - ttl_seconds

        db.transaction do
          stale_workers = db[workers_table]
            .where { last_seen <= cutoff }
            .select(:id)

          ds = db[offsets_table]
            .where(claimed: true)
            .where { claimed_at <= cutoff }
            .where(claimed_by: stale_workers)

          ds.update(claimed: false, claimed_at: nil, claimed_by: nil)
        end
      end

      # Append one or more messages to a stream, creating the stream if it doesn't exist.
      # Also automatically increments the sequence number for the stream and messages,
      # without relying on the client passing the right sequence numbers.
      # This is used for appending messages in order without client-side optimistic concurrency control.
      # For example commands that will be picked up and handled by a reactor, later.
      # @param stream_id [String] Unique identifier for the message stream
      # @param messages [Sourced::Message, Array<Sourced::Message>] Message(s) to append to the stream
      # @option max_retries [Integer] Maximum number of retries on unique constraint violation (default: 3)
      def append_next_to_stream(stream_id, messages, max_retries: 3)
        # Handle both single message and array of messages
        messages_array = Array(messages)
        return true if messages_array.empty?

        retries = 0
        begin
          db.transaction do
            # Update or create a stream.
            # If updating, increment seq by the number of messages being added.
            messages_count = messages_array.size
            stream_record = db[streams_table]
              .insert_conflict(
                target: :stream_id, 
                update: { 
                  seq: Sequel.qualify(streams_table, :seq) + messages_count,
                  updated_at: Time.now 
                }
              )
              .returning(:id, :seq)
              .insert(stream_id:, seq: messages_count)
              .first

            # Calculate the starting sequence number for this batch
            # If stream existed, seq will be the new max seq after increment
            # If stream was created, seq will be messages_count
            starting_seq = stream_record[:seq] - messages_count + 1

            # Prepare rows for bulk insert with incrementing sequence numbers
            rows = messages_array.map.with_index do |msg, index|
              row = serialize_message(msg, stream_record[:id])
              row[:seq] = starting_seq + index
              row
            end

            # Bulk insert all messages
            db[messages_table].multi_insert(rows)
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

      def append_to_stream(stream_id, messages)
        # Handle both single message and array of messages
        messages_array = Array(messages)
        return false if messages_array.empty?

        if messages_array.map(&:stream_id).uniq.size > 1
          raise ArgumentError, 'Messages must all belong to the same stream'
        end

        db.transaction do
          seq = messages_array.last.seq
          id = db[streams_table].insert_conflict(target: :stream_id, update: { seq:, updated_at: Time.now }).insert(stream_id:, seq:)
          rows = messages_array.map { |e| serialize_message(e, id) }
          db[messages_table].multi_insert(rows)
        end

        true
      rescue Sequel::UniqueConstraintViolation => e
        raise Sourced::ConcurrentAppendError, e.message
      end

      # Reserve next message for a reactor, based on the reactor's #handled_messages list
      # This fetches the next un-acknowledged message for the reactor, and processes it in a transaction
      # which aquires a lock on the message's stream_id and the reactor's group_id
      # So that no other reactor instance in the same group can process the same stream concurrently.
      # This way, messages for the same stream are guaranteed to be processed sequentially for each reactor group.
      # If the given block returns truthy, the message is marked as acknowledged for this reactor group
      # (ie. it won't be processed again by the same group).
      # If the block returns falsey, the message is NOT acknowledged and will be retried,
      # unless the block also stops the reactor, which is the default behaviour when an exception is raised.
      # See Router#handle_next_message_for_reactor,
      # A boolean is passed to the block to indicate whether the message is being replayed 
      # (ie the reactor group has previously processed it).
      # This is done by incrementing the group's highest_global_seq with every message acknowledged.
      # When the group's offsets are reset (in order to re-process all messages), this value is preserved
      # so that each message's global_seq can be compared against it to determine if the message is being replayed.
      #
      # @example
      #   backend.reserve_next_for_reactor(reactor) do |message, replaying|
      #     # process message here
      #     true # ACK message
      #   end
      #
      # @param reactor [Sourced::ReactorInterface]
      # @option worker_id [String]
      # @yieldparam [Sourced::Message]
      # @yieldparam [Boolean] whether the message is being replayed (ie. it has been processed before)
      # @yieldreturn [Boolean] whether to ACK the message for this reactor group and stream ID.
      def reserve_next_for_reactor(reactor, worker_id: nil, &)
        worker_id ||= [Process.pid, Thread.current.object_id, Fiber.current.object_id].join('-')
        group_id = reactor.consumer_info.group_id
        handled_messages = reactor.handled_messages.map(&:type).uniq
        now = Time.now

        bootstrap_offsets_for(group_id)

        # Phase 1: Claim stream_id/group_id in short transaction
        # This claim includes :message, :claim_id and :replaying
        row = claim_next_message(group_id, handled_messages, now, reactor.consumer_info.start_from.call, worker_id)

        # return reserve_next_for_reactor(reactor, &) if row == :retry
        return unless row

        message = deserialize_message(row)

        begin
          # Phase 2: Process message outside of transaction
          actions = yield(message, row[:replaying])

          # Phase 3: update state depending on result
          # Result handling happens in the same transaction
          # as ACKing the message
          db.transaction do
            process_actions(group_id, row, actions, message)
          end

        rescue StandardError
          release_offset(row[:offset_id])
          raise
        end

        message
      end

      private def release_offset(offset_id)
        db[offsets_table].where(id: offset_id).update(claimed: false, claimed_at: nil, claimed_by: nil)
      end

      private def process_actions(group_id, row, actions, message)
        should_ack = false
        actions = [actions] unless actions.is_a?(Array)
        actions = actions.compact
        # Empty actions is assumed to be an ACK
        return ack_message(group_id, row[:stream_id_fk], row[:global_seq]) if actions.empty?

        actions.each do |action|
          case action
          when Actions::OK
            should_ack = true

          when Actions::Ack
            ack_on(group_id, action.message_id)

          when Actions::RETRY
            release_offset(row[:offset_id])

          else
            action.execute(self, message)
            should_ack = true
          end
        end

        ack_message(group_id, row[:stream_id_fk], row[:global_seq]) if should_ack
      end

      # Insert missing offsets for a consumer group and all streams.
      # This runs in advance of claiming messages for processing
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

      # @param group_id [String] Consumer group ID to claim the next message for
      # @param handle_messages [Array<String>] List of message types to handle
      # @param now [Time] Current time
      # @param start_from [Time] Optional starting point for message processing
      # @param worker_id [String] Unique identifier for the worker claiming the message
      private def claim_next_message(group_id, handled_messages, now, start_from, worker_id)
        db.transaction do
          # 1. get next message for this group_id and handled_messages
          # Use FOR UPDATE SKIP LOCKED if supported
          row = if start_from.is_a?(Time)
            db.fetch(sql_for_reserve_next_with_messages(handled_messages, group_id:, now:, start_from:)).first
          else
            db.fetch(sql_for_reserve_next_with_messages(handled_messages, group_id:, now:)).first
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
        # Another worker has already claimed this message
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
        group_id = group_id.consumer_info.group_id if group_id.respond_to?(:consumer_info)
        dataset = db[consumer_groups_table].where(group_id: group_id)
        dataset.update(status: ACTIVE, retry_at: nil, error_context: nil)
      end

      # @param group_id [String]
      # @param reason [#inspect, NilClass]
      def stop_consumer_group(group_id, reason = nil)
        group_id = group_id.consumer_info.group_id if group_id.respond_to?(:consumer_info)
        updating_consumer_group(group_id) do |group|
          group.stop(reason)
        end
      end

      # Reset offsets for all streams tracked by a consumer group.
      # If the consumer group is active, this will make it re-process all messages
      #
      # @param group_id [String]
      # @return [Boolean]
      def reset_consumer_group(group_id)
        group_id = group_id.consumer_info.group_id if group_id.respond_to?(:consumer_info)
        db.transaction do
          row = db[consumer_groups_table].where(group_id:).select(:id).first
          return unless row

          id = row[:id]
          db[offsets_table].where(group_id: id).delete
        end

        true
      end

      def ack_on(group_id, message_id, &)
        db.transaction do
          row = db.fetch(sql_for_ack_on, group_id, message_id).first
          raise Sourced::ConcurrentAckError, "Stream for message #{message_id} is being concurrently processed by #{group_id}" unless row

          yield if block_given?

          ack_message(group_id, row[:stream_id_fk], row[:global_seq])
        end
      end

      private def ack_message(group_id, stream_id, global_seq)
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
          # an offset will always exist for an message
          # but the synchronous ack_on method can be used with an message
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

      private def base_messages_query
        db[messages_table]
          .select(
            Sequel[messages_table][:id],
            Sequel[streams_table][:stream_id],
            Sequel[messages_table][:seq],
            Sequel[messages_table][:global_seq],
            Sequel[messages_table][:type],
            Sequel[messages_table][:created_at],
            Sequel[messages_table][:causation_id],
            Sequel[messages_table][:correlation_id],
            Sequel[messages_table][:metadata],
            Sequel[messages_table][:payload],
          )
          .join(streams_table, id: :stream_id)
      end

      def read_correlation_batch(message_id)
        correlation_subquery = db[messages_table]
          .select(:correlation_id)
          .where(id: message_id)

        query = base_messages_query
          .where(Sequel[messages_table][:correlation_id] => correlation_subquery)
          .order(Sequel[messages_table][:global_seq])

        query.map do |row|
          deserialize_message(row)
        end
      end

      def read_stream(stream_id, after: nil, upto: nil)
        _messages_table = messages_table # need local variable for Sequel block

        query = base_messages_query.where(Sequel[streams_table][:stream_id] => stream_id)

        query = query.where { Sequel[_messages_table][:seq] > after } if after
        query = query.where { Sequel[_messages_table][:seq] <= upto } if upto
        query.order(Sequel[_messages_table][:global_seq]).map do |row|
          deserialize_message(row)
        end
      end

      # For tests only
      def clear!
        raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'
        # Truncate and restart global_seq increment first
        db[messages_table].truncate(cascade: true, only: true, restart: true)
        db[messages_table].delete
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

      def copy_migration_to(dir = nil, &block)
        installer.copy_migration_to(dir, &block)
      end

      private

      attr_reader :db, :logger, :prefix, :schema, :messages_table, :streams_table, :offsets_table, :consumer_groups_table, :scheduled_messages_table, :workers_table

      def installer
        @installer ||= Installer.new(
          @db,
          logger:,
          schema:,
          prefix:
        )
      end

      def sql_for_reserve_next_with_messages(handled_messages, group_id:, now:, start_from: nil)
        message_types = handled_messages.map { |e| "'#{e}'" }
        now = db.literal(now)
        group_id = db.literal(group_id)
        message_types_sql = message_types.any? ? " AND e.type IN(#{message_types.join(',')})" : ''
        time_window_sql = start_from ? " AND e.created_at > #{db.literal(start_from)}" : ''

        <<~SQL
          SELECT
              e.global_seq, e.id, ss.stream_id, e.stream_id as stream_id_fk,
              e.seq, e.type, e.created_at, e.causation_id, e.correlation_id,
              e.metadata, e.payload, so.id as offset_id, cg.id as group_id_fk,
              (e.global_seq <= cg.highest_global_seq) AS replaying
          FROM #{@messages_table_literal} e
          JOIN #{@streams_table_literal} ss ON e.stream_id = ss.id
          JOIN #{@consumer_groups_table_literal} cg ON cg.group_id = #{group_id}
          JOIN #{@offsets_table_literal} so ON cg.id = so.group_id AND ss.id = so.stream_id
          WHERE e.global_seq > so.global_seq
            AND so.claimed = FALSE
            AND cg.status = 'active'
            AND (cg.retry_at IS NULL OR cg.retry_at <= #{now})
            #{message_types_sql}#{time_window_sql}
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
            FROM #{@messages_table_literal} e
            JOIN #{@streams_table_literal} s ON e.stream_id = s.id
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
          FROM #{@consumer_groups_table_literal} r
          LEFT JOIN #{@offsets_table_literal} o ON o.group_id = r.id AND o.global_seq > 0
          GROUP BY r.id, r.group_id, r.status, r.retry_at
          ORDER BY r.group_id;
        SQL
      end

      def table_name(name)
        t = [prefix, name].join('_').to_sym
        schema ? Sequel[schema.to_sym][t] : t
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

      def serialize_message(message, stream_id)
        row = message.to_h
        row[:stream_id] = stream_id
        row[:metadata] = JSON.dump(row[:metadata]) if row[:metadata]
        row[:payload] = JSON.dump(row[:payload]) if row[:payload]
        row
      end

      def deserialize_message(row)
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
