# frozen_string_literal: true

require 'sourced/backends/sequel_backend'

module Sourced
  module Backends
    # SQLite-specific backend. Inherits shared behaviour from {SequelBackend}
    # and overrides methods that rely on PG-specific features:
    #
    # - No CTEs with +UPDATE ... RETURNING+ — uses separate SELECT/UPDATE steps
    # - No +FOR UPDATE SKIP LOCKED+ — relies on SQLite's transaction-level write lock
    # - No advisory locks — +ack_on+ runs inside a plain transaction
    # - No +TRUNCATE+ — +clear!+ uses +DELETE+
    # - No +greatest()+ — uses +max()+ instead
    #
    # Uses {InlineNotifier} for synchronous worker dispatch (no PG +LISTEN/NOTIFY+).
    # The {CatchUpPoller} provides a safety-net poll for startup catch-up and missed signals.
    #
    # Best suited for single-process deployments, development, scripts, and tests.
    #
    # @example File-based database
    #   db = Sequel.sqlite('myapp.db')
    #   backend = Sourced::Backends::SQLiteBackend.new(db)
    #   backend.install unless backend.installed?
    #
    # @example In-memory database (useful for scripts and tests)
    #   db = Sequel.sqlite
    #   backend = Sourced::Backends::SQLiteBackend.new(db)
    #
    # @see SequelBackend
    class SQLiteBackend < SequelBackend
      # Move scheduled messages whose +available_at+ has passed into the main message log.
      #
      # Unlike the PG version, this does not use +FOR UPDATE SKIP LOCKED+ to prevent
      # concurrent workers from selecting the same rows. Instead, SQLite's database-level
      # write lock serializes concurrent callers.
      #
      # @return [Integer] number of scheduled messages promoted
      def update_schedule!
        now = Time.now

        transaction do
          rows = db[scheduled_messages_table]
            .where { available_at <= now }
            .order(:id)
            .limit(100)
            .all

          return 0 if rows.empty?

          messages = rows.map do |r|
            data = JSON.parse(r[:message], symbolize_names: true)
            data[:created_at] = now
            Message.from(data)
          end

          messages.group_by(&:stream_id).each do |stream_id, stream_messages|
            append_next_to_stream(stream_id, stream_messages)
          end

          row_ids = rows.map { |m| m[:id] }
          db[scheduled_messages_table].where(id: row_ids).delete

          rows.size
        end
      end

      # Append messages to a stream, auto-incrementing sequence numbers.
      #
      # SQLite doesn't support +RETURNING+ on +INSERT ... ON CONFLICT+, so the stream
      # record is fetched with a separate +SELECT+ after the upsert. Retries on
      # +UniqueConstraintViolation+ with linear backoff up to +max_retries+.
      #
      # @param stream_id [String] the stream to append to
      # @param messages [Message, Array<Message>] messages to append
      # @param max_retries [Integer] retry limit for constraint violations (default: 3)
      # @return [true]
      # @raise [Sourced::ConcurrentAppendError] after exhausting retries
      def append_next_to_stream(stream_id, messages, max_retries: 3)
        messages_array = Array(messages)
        return true if messages_array.empty?

        retries = 0
        begin
          db.transaction do
            messages_count = messages_array.size

            # Upsert stream — SQLite doesn't support RETURNING on INSERT
            db[streams_table]
              .insert_conflict(
                target: :stream_id,
                update: {
                  seq: Sequel.qualify(streams_table, :seq) + messages_count,
                  updated_at: Time.now
                }
              )
              .insert(stream_id:, seq: messages_count)

            stream_record = db[streams_table].where(stream_id:).select(:id, :seq).first

            starting_seq = stream_record[:seq] - messages_count + 1

            rows = messages_array.map.with_index do |msg, index|
              row = serialize_message(msg, stream_record[:id])
              row[:seq] = starting_seq + index
              row
            end

            db[messages_table].multi_insert(rows)

            notifier.notify_new_messages(messages_array.map(&:type))
          end

          true
        rescue Sequel::UniqueConstraintViolation => e
          retries += 1
          if retries <= max_retries
            sleep(0.001 * retries)
            retry
          else
            raise Sourced::ConcurrentAppendError, e.message
          end
        end
      end

      # Append messages to a stream with optimistic locking (sequence check).
      #
      # Messages must carry pre-assigned +seq+ values. A +UniqueConstraintViolation+
      # on the +[stream_id, seq]+ index signals a concurrent append.
      #
      # @param stream_id [String] the stream to append to
      # @param messages [Message, Array<Message>] messages with pre-assigned sequences
      # @return [true]
      # @raise [Sourced::ConcurrentAppendError] on sequence conflict
      def append_to_stream(stream_id, messages)
        messages_array = Array(messages)
        return false if messages_array.empty?

        if messages_array.map(&:stream_id).uniq.size > 1
          raise ArgumentError, 'Messages must all belong to the same stream'
        end

        db.transaction do
          seq = messages_array.last.seq
          db[streams_table]
            .insert_conflict(target: :stream_id, update: { seq:, updated_at: Time.now })
            .insert(stream_id:, seq:)

          # Separate SELECT — SQLite insert_conflict doesn't reliably return the id
          stream_row = db[streams_table].where(stream_id:).select(:id).first
          id = stream_row[:id]

          rows = messages_array.map { |e| serialize_message(e, id) }
          db[messages_table].multi_insert(rows)

          notifier.notify_new_messages(messages_array.map(&:type))
        end

        true
      rescue Sequel::UniqueConstraintViolation => e
        raise Sourced::ConcurrentAppendError, e.message
      end

      # Yield a {GroupUpdater} for the given consumer group inside a transaction.
      #
      # The PG version uses +FOR UPDATE+ to row-lock the group. SQLite relies on its
      # database-level transaction lock instead.
      #
      # @param group_id [String] consumer group identifier
      # @yield [GroupUpdater] group updater for setting retry_at, stop, etc.
      # @raise [ArgumentError] if the consumer group is not found
      def updating_consumer_group(group_id, &)
        db.transaction do
          dataset = db[consumer_groups_table].where(group_id:)
          group_row = dataset.first
          raise ArgumentError, "Consumer group #{group_id} not found" unless group_row

          ctx = group_row[:error_context] ? parse_json(group_row[:error_context]) : {}
          group_row[:error_context] = ctx
          group = GroupUpdater.new(group_id, group_row, logger)
          yield group
          updates = group.updates
          updates[:error_context] = JSON.dump(updates[:error_context])
          dataset.update(updates)
        end
      end

      # Acknowledge a message for a consumer group, running an optional block
      # inside the same transaction.
      #
      # The PG version uses +pg_try_advisory_xact_lock+ to prevent concurrent
      # processing of the same stream by the same group. SQLite relies on the
      # database-level write lock instead, so a second concurrent caller will
      # block until the first commits.
      #
      # @param group_id [String] consumer group identifier
      # @param message_id [String] UUID of the message to acknowledge
      # @yield optional block to run inside the transaction before ACKing
      # @raise [Sourced::ConcurrentAckError] if the message is not found
      def ack_on(group_id, message_id, &)
        db.transaction do
          row = db[messages_table]
            .join(streams_table, id: :stream_id)
            .where(Sequel[messages_table][:id] => message_id)
            .select(
              Sequel[messages_table][:global_seq],
              Sequel[messages_table][:stream_id].as(:stream_id_fk)
            )
            .first

          raise Sourced::ConcurrentAckError, "Message #{message_id} not found for group #{group_id}" unless row

          yield if block_given?

          ack_message(group_id, row[:stream_id_fk], row[:global_seq])
        end
      end

      # Clear all data. For tests only.
      #
      # Uses +DELETE+ instead of +TRUNCATE+ (not supported by SQLite).
      # Also resets the +sqlite_sequence+ table so +global_seq+ auto-increment
      # restarts from 1.
      #
      # @raise [RuntimeError] if +ENV['ENVIRONMENT']+ is not +'test'+
      # @return [void]
      def clear!
        raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'
        db[offsets_table].delete
        db[messages_table].delete
        db[consumer_groups_table].delete
        db[streams_table].delete
        db[scheduled_messages_table].delete
        db[workers_table].delete
        # Reset auto-increment counters so global_seq starts from 1
        db.run("DELETE FROM sqlite_sequence") if db.table_exists?(:sqlite_sequence)
      end

      protected

      # Configure SQLite-specific PRAGMAs and set up the {InlineNotifier}.
      #
      # - +foreign_keys = ON+ — enforce FK constraints (off by default in SQLite)
      # - +journal_mode = WAL+ — allow concurrent reads during writes
      # - +busy_timeout = 5000+ — wait up to 5 s for a write lock before raising
      #
      # @return [void]
      def setup_adapter
        @notifier = InlineNotifier.new
        @db.run('PRAGMA foreign_keys = ON')
        @db.run('PRAGMA journal_mode = WAL')
        @db.run('PRAGMA busy_timeout = 5000')
      end

      private

      # @return [String] filename of the SQLite-specific migration ERB template
      def migration_template_name
        '001_create_sourced_tables_sqlite.rb.erb'
      end

      # Acknowledge a message by advancing the consumer group's offset.
      #
      # Uses +max()+ instead of PG's +greatest()+ to update +highest_global_seq+.
      # Falls back to +INSERT+ if the consumer group row doesn't exist yet.
      #
      # @param group_id [String] consumer group identifier
      # @param stream_id [Integer] stream FK (internal integer ID, not the string stream_id)
      # @param global_seq [Integer] global sequence number of the acknowledged message
      # @return [void]
      def ack_message(group_id, stream_id, global_seq)
        db.transaction do
          # Update consumer group — use max() instead of greatest()
          updated = db[consumer_groups_table]
            .where(group_id:)
            .update(
              updated_at: Time.now,
              highest_global_seq: Sequel.function(:max, :highest_global_seq, global_seq)
            )

          if updated == 0
            db[consumer_groups_table].insert(group_id:, highest_global_seq: global_seq)
          end

          group_id_fk = db[consumer_groups_table].where(group_id:).select(:id).first[:id]

          # Upsert offset
          db[offsets_table]
            .insert_conflict(
              target: [:group_id, :stream_id],
              update: { claimed: false, claimed_at: nil, claimed_by: nil, global_seq: }
            )
            .insert(
              group_id: group_id_fk,
              stream_id:,
              global_seq:
            )
        end
      end

      # Decompose the PG CTE (claim + fetch in one statement) into
      # separate SELECT, UPDATE, SELECT steps within a transaction.
      #
      # SQLite has no +FOR UPDATE SKIP LOCKED+, so two concurrent workers
      # contend for the database-level write lock rather than skipping locked rows.
      #
      # @param group_id [String] consumer group identifier
      # @param handled_messages [Array<String>] message type strings to filter
      # @param now [Time] current time for claim timestamps and retry_at checks
      # @param start_from [Time, nil] optional time window lower bound
      # @param worker_id [String] identifier for the claiming worker
      # @param batch_size [Integer] maximum messages to fetch
      # @param with_history [Boolean] whether to include full stream history
      # @return [Array(Array<Hash>, Array<Message>?), NO_BATCH] batch rows and optional history
      def claim_and_fetch_batch(group_id, handled_messages, now, start_from, worker_id, batch_size, with_history: false)
        message_types_sql = handled_messages.any? ? " AND e.type IN(#{handled_messages.map { |e| db.literal(e) }.join(',')})" : ''
        time_window_sql = start_from.is_a?(Time) ? " AND e.created_at > #{db.literal(start_from)}" : ''

        db.transaction do
          # Step 1: Find first unclaimed offset with a matching pending message
          first_match = db.fetch(<<~SQL).first
            SELECT
              so.id as offset_id, ss.stream_id, e.stream_id as stream_id_fk,
              cg.id as group_id_fk,
              cg.highest_global_seq,
              so.global_seq as offset_seq
            FROM #{@messages_table_literal} e
            JOIN #{@streams_table_literal} ss ON e.stream_id = ss.id
            JOIN #{@consumer_groups_table_literal} cg ON cg.group_id = #{db.literal(group_id)}
            JOIN #{@offsets_table_literal} so ON cg.id = so.group_id AND ss.id = so.stream_id
            WHERE e.global_seq > so.global_seq
              AND so.claimed = 0
              AND cg.status = 'active'
              AND (cg.retry_at IS NULL OR cg.retry_at <= #{db.literal(now)})
              #{message_types_sql}#{time_window_sql}
            ORDER BY e.global_seq ASC
            LIMIT 1
          SQL

          return NO_BATCH unless first_match

          # Step 2: Claim the offset
          claimed = db[offsets_table]
            .where(id: first_match[:offset_id], claimed: false)
            .update(claimed: true, claimed_at: now, claimed_by: worker_id)

          return NO_BATCH if claimed == 0

          # Step 3: Fetch messages
          if with_history
            fetch_batch_with_history(first_match, message_types_sql, time_window_sql, batch_size)
          else
            fetch_batch(first_match, message_types_sql, time_window_sql, batch_size)
          end
        end
      rescue Sequel::UniqueConstraintViolation
        logger.debug "Batch claim for group #{group_id} already exists, skipping"
        NO_BATCH
      end

      # Fetch a batch of messages for the claimed offset.
      #
      # @param first_match [Hash] the claimed offset row
      # @param message_types_sql [String] SQL fragment filtering message types
      # @param time_window_sql [String] SQL fragment filtering by time
      # @param batch_size [Integer] maximum messages to fetch
      # @return [Array(Array<Hash>, nil), NO_BATCH]
      def fetch_batch(first_match, message_types_sql, time_window_sql, batch_size)
        rows = db.fetch(<<~SQL).all
          SELECT e.global_seq, e.id, #{db.literal(first_match[:stream_id])} as stream_id,
                 e.stream_id as stream_id_fk,
                 e.seq, e.type, e.created_at, e.causation_id, e.correlation_id,
                 e.metadata, e.payload, #{db.literal(first_match[:offset_id])} as offset_id,
                 #{db.literal(first_match[:group_id_fk])} as group_id_fk,
                 (e.global_seq <= #{db.literal(first_match[:highest_global_seq])}) AS replaying,
                 #{db.literal(first_match[:highest_global_seq])} as highest_global_seq
          FROM #{@messages_table_literal} e
          WHERE e.stream_id = #{db.literal(first_match[:stream_id_fk])}
            AND e.global_seq > #{db.literal(first_match[:offset_seq])}
            #{message_types_sql}#{time_window_sql}
          ORDER BY e.global_seq ASC
          LIMIT #{db.literal(batch_size)}
        SQL

        return NO_BATCH if rows.empty?

        coerce_booleans!(rows)
        [rows, nil]
      end

      # Fetch a batch of messages along with full stream history.
      #
      # @param first_match [Hash] the claimed offset row
      # @param message_types_sql [String] SQL fragment filtering message types
      # @param time_window_sql [String] SQL fragment filtering by time
      # @param batch_size [Integer] maximum messages to fetch
      # @return [Array(Array<Hash>, Array<Message>), NO_BATCH]
      def fetch_batch_with_history(first_match, message_types_sql, time_window_sql, batch_size)
        # Fetch all stream messages for history
        all_rows = db.fetch(<<~SQL).all
          SELECT e.global_seq, e.id, #{db.literal(first_match[:stream_id])} as stream_id,
                 e.stream_id as stream_id_fk,
                 e.seq, e.type, e.created_at, e.causation_id, e.correlation_id,
                 e.metadata, e.payload, #{db.literal(first_match[:offset_id])} as offset_id,
                 #{db.literal(first_match[:group_id_fk])} as group_id_fk,
                 (e.global_seq <= #{db.literal(first_match[:highest_global_seq])}) AS replaying,
                 #{db.literal(first_match[:highest_global_seq])} as highest_global_seq
          FROM #{@messages_table_literal} e
          WHERE e.stream_id = #{db.literal(first_match[:stream_id_fk])}
          ORDER BY e.global_seq ASC
        SQL

        return NO_BATCH if all_rows.empty?

        # Identify which rows are in the batch
        batch_seqs = db.fetch(<<~SQL).map { |r| r[:global_seq] }
          SELECT e.global_seq
          FROM #{@messages_table_literal} e
          WHERE e.stream_id = #{db.literal(first_match[:stream_id_fk])}
            AND e.global_seq > #{db.literal(first_match[:offset_seq])}
            #{message_types_sql}#{time_window_sql}
          ORDER BY e.global_seq ASC
          LIMIT #{db.literal(batch_size)}
        SQL

        batch_set = batch_seqs.to_set
        all_rows.each { |r| r[:in_batch] = batch_set.include?(r[:global_seq]) }

        coerce_booleans!(all_rows)

        batch_rows = all_rows.select { |r| r[:in_batch] }
        if batch_rows.empty?
          release_offset(first_match[:offset_id])
          return NO_BATCH
        end

        history = all_rows.map { |row| deserialize_message(row) }
        [batch_rows, history]
      end

      # Convert SQLite integer booleans (0/1) to Ruby booleans.
      #
      # SQLite boolean expressions like +(expr) AS replaying+ return +0+ or +1+
      # rather than +true+/+false+. This normalizes the +:replaying+ field so
      # downstream code works with Ruby booleans.
      #
      # @param rows [Array<Hash>] rows to mutate in place
      # @return [void]
      def coerce_booleans!(rows)
        rows.each { |r| r[:replaying] = (r[:replaying] == 1 || r[:replaying] == true) }
      end
    end
  end
end
