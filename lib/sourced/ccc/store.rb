# frozen_string_literal: true

require 'json'

module Sourced
  module CCC
    # Wraps a Message with a storage position. Delegates all message methods.
    class PositionedMessage < SimpleDelegator
      attr_reader :position

      def initialize(message, position)
        super(message)
        @position = position
      end

      def class = __getobj__.class
      def is_a?(klass) = __getobj__.is_a?(klass) || super
      def kind_of?(klass) = is_a?(klass)
      def instance_of?(klass) = __getobj__.instance_of?(klass)
    end

    # Returned by {Store#claim_next} with everything needed to process and ack a partition.
    ClaimResult = Data.define(:offset_id, :key_pair_ids, :partition_key, :partition_value, :messages, :replaying, :guard)

    # Returned by {Store#read} with messages and a consistency guard.
    # Supports array destructuring via #to_ary for backwards compatibility:
    #   messages, guard = store.read(conditions)
    ReadResult = Data.define(:messages, :guard) do
      def to_ary = [messages, guard]
    end

    # SQLite-backed store for CCC's flat, globally-ordered message log.
    # Provides message storage with automatic key-pair indexing,
    # consumer group management, and partition-based offset tracking
    # for parallel background processing.
    class Store
      ACTIVE = 'active'
      STOPPED = 'stopped'

      # @return [Sequel::SQLite::Database]
      attr_reader :db

      # @param db [Sequel::SQLite::Database] a Sequel SQLite connection
      def initialize(db)
        @db = db
        @db.run('PRAGMA foreign_keys = ON')
        @db.run('PRAGMA journal_mode = WAL')
        @db.run('PRAGMA busy_timeout = 5000')
      end

      # Whether all required tables exist.
      # @return [Boolean]
      def installed?
        db.table_exists?(:ccc_messages) &&
          db.table_exists?(:ccc_key_pairs) &&
          db.table_exists?(:ccc_message_key_pairs) &&
          db.table_exists?(:ccc_consumer_groups) &&
          db.table_exists?(:ccc_offsets) &&
          db.table_exists?(:ccc_offset_key_pairs)
      end

      # Create all required tables and indexes. Idempotent.
      # @return [void]
      def install!
        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_messages (
            position INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            message_type TEXT NOT NULL,
            causation_id TEXT,
            correlation_id TEXT,
            payload TEXT NOT NULL,
            metadata TEXT,
            created_at TEXT NOT NULL
          )
        SQL
        db.run('CREATE INDEX IF NOT EXISTS idx_ccc_message_type ON ccc_messages(message_type)')

        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_key_pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value TEXT NOT NULL,
            UNIQUE(name, value)
          )
        SQL
        db.run('CREATE INDEX IF NOT EXISTS idx_ccc_key_pair_nv ON ccc_key_pairs(name, value)')

        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_message_key_pairs (
            message_position INTEGER NOT NULL REFERENCES ccc_messages(position),
            key_pair_id INTEGER NOT NULL REFERENCES ccc_key_pairs(id),
            PRIMARY KEY (message_position, key_pair_id)
          )
        SQL
        db.run('CREATE INDEX IF NOT EXISTS idx_ccc_mkp_key ON ccc_message_key_pairs(key_pair_id, message_position)')

        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_consumer_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT '#{ACTIVE}',
            highest_position INTEGER NOT NULL DEFAULT 0,
            error_context TEXT,
            retry_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
          )
        SQL

        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_offsets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            consumer_group_id INTEGER NOT NULL REFERENCES ccc_consumer_groups(id) ON DELETE CASCADE,
            partition_key TEXT NOT NULL,
            last_position INTEGER NOT NULL DEFAULT 0,
            claimed INTEGER NOT NULL DEFAULT 0,
            claimed_at TEXT,
            claimed_by TEXT,
            UNIQUE(consumer_group_id, partition_key)
          )
        SQL

        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_offset_key_pairs (
            offset_id INTEGER NOT NULL REFERENCES ccc_offsets(id) ON DELETE CASCADE,
            key_pair_id INTEGER NOT NULL REFERENCES ccc_key_pairs(id),
            PRIMARY KEY (offset_id, key_pair_id)
          )
        SQL
      end

      # Append messages to the store. Extracts and indexes key-value pairs
      # from each message's payload automatically.
      #
      # When a {ConsistencyGuard} is provided, checks for conflicting messages
      # before inserting (optimistic concurrency).
      #
      # @param messages [CCC::Message, Array<CCC::Message>] one or more messages to append
      # @param guard [ConsistencyGuard, nil] optional guard for conflict detection
      # @return [Integer] the last assigned position
      # @raise [Sourced::ConcurrentAppendError] if conflicting messages found after guard position
      def append(messages, guard: nil)
        messages = Array(messages)
        return latest_position if messages.empty?

        last_position = nil

        db.transaction do
          if guard
            conflicts = check_conflicts(guard.conditions, guard.last_position)
            raise Sourced::ConcurrentAppendError, "Conflicting messages found after position #{guard.last_position}" if conflicts.any?
          end

          messages.each do |msg|
            payload_json = msg.payload ? JSON.dump(msg.payload.to_h) : '{}'
            metadata_json = msg.metadata.empty? ? nil : JSON.dump(msg.metadata)

            db[:ccc_messages].insert(
              message_id: msg.id,
              message_type: msg.type,
              causation_id: msg.causation_id,
              correlation_id: msg.correlation_id,
              payload: payload_json,
              metadata: metadata_json,
              created_at: msg.created_at.iso8601
            )

            last_position = db[:ccc_messages].where(message_id: msg.id).get(:position)

            # Extract and index key pairs
            msg.extracted_keys.each do |name, value|
              db.run("INSERT OR IGNORE INTO ccc_key_pairs (name, value) VALUES (#{db.literal(name)}, #{db.literal(value)})")
              key_pair_id = db[:ccc_key_pairs].where(name: name, value: value).get(:id)

              db[:ccc_message_key_pairs].insert(
                message_position: last_position,
                key_pair_id: key_pair_id
              )
            end
          end
        end

        last_position
      end

      # Query messages by conditions. Each condition matches on
      # (message_type AND key_name AND key_value). Multiple conditions are OR'd.
      #
      # @param conditions [QueryCondition, Array<QueryCondition>] query conditions
      # @param from_position [Integer, nil] only return messages after this position
      # @param limit [Integer, nil] max number of messages to return
      # @return [ReadResult] messages and a guard
      def read(conditions, from_position: nil, limit: nil)
        conditions = Array(conditions)
        if conditions.empty?
          guard = ConsistencyGuard.new(conditions: conditions, last_position: from_position || latest_position)
          return ReadResult.new(messages: [], guard: guard)
        end

        messages = query_messages(conditions, from_position: from_position, limit: limit)
        last_pos = messages.any? ? messages.last.position : (from_position || latest_position)
        guard = ConsistencyGuard.new(conditions: conditions, last_position: last_pos)
        ReadResult.new(messages: messages, guard: guard)
      end

      # Conflict detection: returns messages matching conditions that appeared
      # after the given position. Empty array means no conflicts.
      #
      # @param conditions [Array<QueryCondition>] conditions to check
      # @param position [Integer] check for messages after this position
      # @return [ReadResult]
      def messages_since(conditions, position)
        read(conditions, from_position: position)
      end

      # Register a consumer group. Idempotent.
      #
      # @param group_id [String] unique identifier for the consumer group
      # @return [void]
      def register_consumer_group(group_id)
        now = Time.now.iso8601
        db.run(<<~SQL)
          INSERT OR IGNORE INTO ccc_consumer_groups (group_id, status, highest_position, created_at, updated_at)
          VALUES (#{db.literal(group_id)}, '#{ACTIVE}', 0, #{db.literal(now)}, #{db.literal(now)})
        SQL
      end

      # Whether the consumer group exists and is active.
      #
      # @param group_id [String]
      # @return [Boolean]
      def consumer_group_active?(group_id)
        row = db[:ccc_consumer_groups].where(group_id: group_id).select(:status).first
        return false unless row

        row[:status] == ACTIVE
      end

      # Stop a consumer group. Stopped groups are skipped by {#claim_next}.
      #
      # @param group_id [String]
      # @return [void]
      def stop_consumer_group(group_id)
        db[:ccc_consumer_groups].where(group_id: group_id).update(status: STOPPED, updated_at: Time.now.iso8601)
      end

      # Re-activate a stopped consumer group.
      #
      # @param group_id [String]
      # @return [void]
      def start_consumer_group(group_id)
        db[:ccc_consumer_groups].where(group_id: group_id).update(status: ACTIVE, updated_at: Time.now.iso8601)
      end

      # Delete all offsets for a consumer group, resetting it to process from the beginning.
      #
      # @param group_id [String]
      # @return [void]
      def reset_consumer_group(group_id)
        cg = db[:ccc_consumer_groups].where(group_id: group_id).first
        return unless cg

        db[:ccc_offsets].where(consumer_group_id: cg[:id]).delete
      end

      # Claim the next available partition for processing.
      #
      # Bootstraps partition offsets (discovering new partitions from messages with
      # ALL +partition_by+ attributes), finds the unclaimed partition with the earliest
      # pending message, claims it, and fetches messages using conditional AND semantics.
      #
      # Returns a {ConsistencyGuard} alongside the messages, built from each handled
      # message class's declared payload attributes via {Message.to_conditions}.
      #
      # The +replaying+ flag indicates whether the returned messages have been
      # processed by this consumer group before. A message is replaying when its
      # position is at or before the consumer group's +highest_position+ — the
      # furthest position ever successfully acked. After a reset, re-claimed
      # messages are correctly flagged as replaying.
      #
      # @param group_id [String] consumer group identifier
      # @param partition_by [String, Array<String>] attribute name(s) defining partitions
      # @param handled_types [Array<String>] message type strings this consumer handles
      # @param worker_id [String] identifier for the claiming worker
      # @return [Hash, nil] +{ offset_id:, key_pair_ids:, partition_key:, partition_value:, messages:, replaying:, guard: }+ or nil
      def claim_next(group_id, partition_by:, handled_types:, worker_id:)
        partition_by = Array(partition_by).sort
        cg = db[:ccc_consumer_groups].where(group_id: group_id, status: ACTIVE).first
        return nil unless cg

        bootstrap_offsets(cg[:id], partition_by)

        claimed = find_and_claim_partition(cg[:id], handled_types, worker_id)
        return nil unless claimed

        key_pair_ids = db[:ccc_offset_key_pairs]
          .where(offset_id: claimed[:offset_id])
          .select_map(:key_pair_id)

        messages = fetch_partition_messages(key_pair_ids, claimed[:last_position], handled_types)

        # If no messages pass the conditional AND filter, release and return nil
        if messages.empty?
          release(group_id, offset_id: claimed[:offset_id])
          return nil
        end

        # Build partition_value hash from key_pairs
        partition_value = {}
        db[:ccc_key_pairs].where(id: key_pair_ids).each do |kp|
          partition_value[kp[:name]] = kp[:value]
        end

        # Build guard conditions from handled_types.
        # Each class's to_conditions only generates conditions for attributes it actually has.
        # We use handled_types (not just fetched messages) so the guard also covers
        # message types that haven't appeared yet but would be conflicts.
        partition_attrs = partition_value.transform_keys(&:to_sym)
        guard_conditions = handled_types.filter_map do |type|
          klass = Message.registry[type]
          klass&.to_conditions(**partition_attrs)
        end.flatten

        last_pos = messages.last.position
        guard = ConsistencyGuard.new(conditions: guard_conditions, last_position: last_pos)

        # replaying: true when all messages are at or below the highest position
        # ever acked by this consumer group (i.e. they've been processed before).
        replaying = messages.last.position <= cg[:highest_position]

        ClaimResult.new(
          offset_id: claimed[:offset_id],
          key_pair_ids: key_pair_ids,
          partition_key: claimed[:partition_key],
          partition_value: partition_value,
          messages: messages,
          replaying: replaying,
          guard: guard
        )
      end

      # Acknowledge processing: advance the offset to +position+ and release the claim.
      # Also advances the consumer group's +highest_position+ watermark (never decreases),
      # which drives the {#claim_next} +replaying+ flag.
      #
      # @param group_id [String] consumer group identifier
      # @param offset_id [Integer] offset ID from the claim result
      # @param position [Integer] position of the last processed message
      # @return [void]
      def ack(group_id, offset_id:, position:)
        cg = db[:ccc_consumer_groups].where(group_id: group_id).first
        return unless cg

        db[:ccc_offsets].where(id: offset_id, consumer_group_id: cg[:id]).update(
          last_position: position,
          claimed: 0,
          claimed_at: nil,
          claimed_by: nil
        )

        # Advance the high watermark (never decrease)
        if position > cg[:highest_position]
          db[:ccc_consumer_groups].where(id: cg[:id]).update(
            highest_position: position,
            updated_at: Time.now.iso8601
          )
        end
      end

      # Release a claim without advancing the offset. Use for error recovery
      # so the partition can be re-claimed and retried.
      #
      # @param group_id [String] consumer group identifier
      # @param offset_id [Integer] offset ID from the claim result
      # @return [void]
      def release(group_id, offset_id:)
        cg = db[:ccc_consumer_groups].where(group_id: group_id).first
        return unless cg

        db[:ccc_offsets].where(id: offset_id, consumer_group_id: cg[:id]).update(
          claimed: 0,
          claimed_at: nil,
          claimed_by: nil
        )
      end

      # Current max position in the message log.
      #
      # @return [Integer] max position, or 0 if the store is empty
      def latest_position
        db[:ccc_messages].max(:position) || 0
      end

      # Delete all data from all tables and reset autoincrement. For testing only.
      #
      # @return [void]
      def clear!
        db[:ccc_offset_key_pairs].delete
        db[:ccc_offsets].delete
        db[:ccc_consumer_groups].delete
        db[:ccc_message_key_pairs].delete
        db[:ccc_key_pairs].delete
        db[:ccc_messages].delete
        db.run('DELETE FROM sqlite_sequence') if db.table_exists?(:sqlite_sequence)
      end

      private

      # Build canonical partition key string from attribute names and values.
      # Sorted by attribute name for deterministic uniqueness.
      #
      # @param partition_by [Array<String>] attribute names
      # @param values [Hash{String => String}] attribute values keyed by name
      # @return [String] e.g. "course_name:Algebra|user_id:joe"
      def build_partition_key(partition_by, values)
        partition_by.sort.map { |attr| "#{attr}:#{values[attr]}" }.join('|')
      end

      # Discover partition tuples via AND self-joins and create offset + key_pair rows.
      # Only messages with ALL partition attributes create partition tuples.
      #
      # @param cg_id [Integer] consumer group internal ID
      # @param partition_by [Array<String>] sorted attribute names
      # @return [void]
      def bootstrap_offsets(cg_id, partition_by)
        # Build AND self-join query to find all unique tuples
        joins = []
        selects = []
        partition_by.each_with_index do |attr, i|
          joins << "JOIN ccc_message_key_pairs mkp#{i} ON m.position = mkp#{i}.message_position"
          joins << "JOIN ccc_key_pairs kp#{i} ON mkp#{i}.key_pair_id = kp#{i}.id AND kp#{i}.name = #{db.literal(attr)}"
          selects << "kp#{i}.id AS kp_id_#{i}, kp#{i}.value AS val_#{i}"
        end

        group_by = partition_by.each_index.map { |i| "kp#{i}.id" }.join(', ')

        sql = <<~SQL
          SELECT #{selects.join(', ')}
          FROM ccc_messages m
          #{joins.join("\n")}
          GROUP BY #{group_by}
        SQL

        db.fetch(sql).each do |row|
          # Build the values hash and collect key_pair_ids
          values = {}
          kp_ids = []
          partition_by.each_with_index do |attr, i|
            values[attr] = row[:"val_#{i}"]
            kp_ids << row[:"kp_id_#{i}"]
          end

          partition_key = build_partition_key(partition_by, values)

          # INSERT OR IGNORE the offset row
          db.run(<<~SQL)
            INSERT OR IGNORE INTO ccc_offsets (consumer_group_id, partition_key, last_position, claimed)
            VALUES (#{db.literal(cg_id)}, #{db.literal(partition_key)}, 0, 0)
          SQL

          offset_id = db[:ccc_offsets].where(consumer_group_id: cg_id, partition_key: partition_key).get(:id)

          # INSERT OR IGNORE the offset_key_pairs join rows
          kp_ids.each do |kp_id|
            db.run(<<~SQL)
              INSERT OR IGNORE INTO ccc_offset_key_pairs (offset_id, key_pair_id)
              VALUES (#{db.literal(offset_id)}, #{db.literal(kp_id)})
            SQL
          end
        end
      end

      # Find the next unclaimed partition with pending messages and claim it.
      # Uses OR semantics for detection (any matching key_pair is sufficient);
      # exact conditional AND filtering happens at fetch time.
      #
      # @param cg_id [Integer] consumer group internal ID
      # @param handled_types [Array<String>] message type strings
      # @param worker_id [String] claiming worker identifier
      # @return [Hash, nil] +{ offset_id:, partition_key:, last_position: }+ or nil
      def find_and_claim_partition(cg_id, handled_types, worker_id)
        types_list = handled_types.map { |t| db.literal(t) }.join(', ')

        sql = <<~SQL
          SELECT o.id AS offset_id, o.partition_key, o.last_position,
                 MIN(m.position) AS next_position
          FROM ccc_offsets o
          JOIN ccc_offset_key_pairs okp ON o.id = okp.offset_id
          JOIN ccc_message_key_pairs mkp ON okp.key_pair_id = mkp.key_pair_id
          JOIN ccc_messages m ON mkp.message_position = m.position
          WHERE o.consumer_group_id = #{db.literal(cg_id)}
            AND o.claimed = 0
            AND m.position > o.last_position
            AND m.message_type IN (#{types_list})
          GROUP BY o.id
          ORDER BY next_position ASC
          LIMIT 1
        SQL

        row = db.fetch(sql).first
        return nil unless row

        now = Time.now.iso8601
        updated = db[:ccc_offsets]
          .where(id: row[:offset_id], claimed: 0)
          .update(claimed: 1, claimed_at: now, claimed_by: worker_id)

        return nil if updated == 0

        { offset_id: row[:offset_id], partition_key: row[:partition_key], last_position: row[:last_position] }
      end

      # Fetch messages for a partition using conditional AND semantics.
      # For each candidate message, it must match ALL of the partition's attributes
      # that the message itself has. Messages with a single partition attribute match
      # on that one; messages with multiple must match all of them.
      #
      # @param key_pair_ids [Array<Integer>] partition key_pair IDs
      # @param last_position [Integer] fetch messages after this position
      # @param handled_types [Array<String>] message type strings
      # @return [Array<PositionedMessage>]
      def fetch_partition_messages(key_pair_ids, last_position, handled_types)
        return [] if key_pair_ids.empty?

        kp_ids_list = key_pair_ids.map { |id| db.literal(id) }.join(', ')
        types_list = handled_types.map { |t| db.literal(t) }.join(', ')

        sql = <<~SQL
          SELECT DISTINCT m.position, m.message_id, m.message_type, m.causation_id, m.correlation_id, m.payload, m.metadata, m.created_at
          FROM ccc_messages m
          WHERE m.position > #{db.literal(last_position)}
            AND m.message_type IN (#{types_list})
            AND EXISTS (
              SELECT 1 FROM ccc_message_key_pairs mkp
              WHERE mkp.message_position = m.position
                AND mkp.key_pair_id IN (#{kp_ids_list})
            )
            AND (
              SELECT COUNT(*) FROM ccc_message_key_pairs mkp
              WHERE mkp.message_position = m.position
                AND mkp.key_pair_id IN (#{kp_ids_list})
            ) = (
              SELECT COUNT(DISTINCT kp_part.name)
              FROM ccc_message_key_pairs mkp2
              JOIN ccc_key_pairs kp_msg ON mkp2.key_pair_id = kp_msg.id
              JOIN ccc_key_pairs kp_part ON kp_part.id IN (#{kp_ids_list})
                AND kp_part.name = kp_msg.name
              WHERE mkp2.message_position = m.position
            )
          ORDER BY m.position ASC
        SQL

        db.fetch(sql).map { |row| deserialize(row) }
      end

      # Core query logic shared by {#read} and {#check_conflicts}.
      # Resolves key_pair IDs from conditions, then queries messages via OR'd clauses.
      #
      # @param conditions [Array<QueryCondition>]
      # @param from_position [Integer, nil]
      # @param limit [Integer, nil]
      # @return [Array<PositionedMessage>]
      def query_messages(conditions, from_position: nil, limit: nil)
        # Step 1: resolve key_pair IDs
        key_lookups = conditions.map { |c| [c.key_name, c.key_value] }.uniq
        or_clauses = key_lookups.map { |n, v| "(name = #{db.literal(n)} AND value = #{db.literal(v)})" }
        key_rows = db.fetch("SELECT id, name, value FROM ccc_key_pairs WHERE #{or_clauses.join(' OR ')}").all

        key_pair_index = {}
        key_rows.each { |r| key_pair_index[[r[:name], r[:value]]] = r[:id] }

        # Build condition clauses using resolved key_pair IDs
        where_parts = conditions.filter_map do |c|
          kp_id = key_pair_index[[c.key_name, c.key_value]]
          next unless kp_id # key pair not in DB means no matches for this condition

          "(m.message_type = #{db.literal(c.message_type)} AND mkp.key_pair_id = #{db.literal(kp_id)})"
        end

        return [] if where_parts.empty?

        sql = <<~SQL
          SELECT DISTINCT m.position, m.message_id, m.message_type, m.causation_id, m.correlation_id, m.payload, m.metadata, m.created_at
          FROM ccc_messages m
          JOIN ccc_message_key_pairs mkp ON m.position = mkp.message_position
          WHERE (#{where_parts.join(' OR ')})
        SQL

        sql += " AND m.position > #{db.literal(from_position)}" if from_position
        sql += ' ORDER BY m.position'
        sql += " LIMIT #{db.literal(limit)}" if limit

        db.fetch(sql).map { |row| deserialize(row) }
      end

      # Check for conflicting messages after a given position.
      #
      # @param conditions [Array<QueryCondition>]
      # @param after_position [Integer]
      # @return [Array<PositionedMessage>]
      def check_conflicts(conditions, after_position)
        return [] if conditions.empty?

        query_messages(conditions, from_position: after_position)
      end

      # Deserialize a database row into a {PositionedMessage}.
      # Looks up the message class from the registry; falls back to base {Message}.
      #
      # @param row [Hash] database row with :position, :message_id, :message_type, :causation_id, :correlation_id, :payload, :metadata, :created_at
      # @return [PositionedMessage]
      def deserialize(row)
        payload = JSON.parse(row[:payload], symbolize_names: true)
        metadata = row[:metadata] ? JSON.parse(row[:metadata], symbolize_names: true) : {}

        klass = Message.registry[row[:message_type]]
        attrs = {
          id: row[:message_id],
          type: row[:message_type],
          causation_id: row[:causation_id],
          correlation_id: row[:correlation_id],
          created_at: row[:created_at],
          metadata: metadata,
          payload: payload
        }

        msg = if klass
                klass.new(attrs)
              else
                Message.new(attrs)
              end

        PositionedMessage.new(msg, row[:position])
      end
    end
  end
end
