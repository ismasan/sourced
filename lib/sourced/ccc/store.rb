# frozen_string_literal: true

require 'json'
require 'set'
require 'sourced/inline_notifier'
require 'sourced/ccc/installer'

module Sourced
  module CCC
    # Wraps a Message with a storage position. Delegates all message methods.
    class PositionedMessage < SimpleDelegator
      attr_reader :position

      # @param message [CCC::Message] the wrapped message instance
      # @param position [Integer] global log position
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

    ReadAllResult = Data.define(:messages, :last_position, :fetcher) do
      include Enumerable

      def to_ary = [messages, last_position]

      # Iterates messages in the current page.
      def each(&block) = messages.each(&block)

      # Returns an Enumerator that lazily paginates through all messages,
      # fetching subsequent pages as needed.
      def to_enum
        Enumerator.new do |y|
          result = self
          loop do
            break if result.messages.empty?

            result.messages.each { |m| y << m }
            result = result.fetcher.call(result.messages.last.position)
          end
        end
      end
    end

    Stats = Data.define(:max_position, :groups)

    # SQLite-backed store for CCC's flat, globally-ordered message log.
    # Provides message storage with automatic key-pair indexing,
    # consumer group management, and partition-based offset tracking
    # for parallel background processing.
    class Store
      ACTIVE = 'active'
      STOPPED = 'stopped'
      FAILED = 'failed'
      DISCOVERY_BATCH_SIZE = 100

      # @return [Sequel::SQLite::Database]
      attr_reader :db

      # @return [Sourced::InlineNotifier]
      attr_reader :notifier

      # @return [Logger]
      attr_reader :logger

      # @return [CCC::Installer]
      attr_reader :installer

      # @param db [Sequel::SQLite::Database] a Sequel SQLite connection
      # @param notifier [#notify_new_messages, #notify_reactor_resumed, nil] optional notifier for dispatch signals
      # @param logger [Logger, nil] optional logger (defaults to Sourced.config.logger)
      # @param prefix [String] table name prefix (default 'sourced')
      def initialize(db, notifier: nil, logger: nil, prefix: 'sourced')
        @db = db
        @notifier = notifier || Sourced::InlineNotifier.new
        @logger = logger || Sourced.config.logger
        Sequel.extension(:fiber_concurrency)
        @db.run('PRAGMA foreign_keys = ON')
        @db.run('PRAGMA journal_mode = WAL')
        @db.run('PRAGMA busy_timeout = 5000')

        @installer = Installer.new(db, logger: @logger, prefix: prefix)

        # Source table name symbols from the installer
        @messages_table          = @installer.messages_table
        @key_pairs_table         = @installer.key_pairs_table
        @message_key_pairs_table = @installer.message_key_pairs_table
        @scheduled_messages_table = @installer.scheduled_messages_table
        @consumer_groups_table   = @installer.consumer_groups_table
        @offsets_table           = @installer.offsets_table
        @offset_key_pairs_table  = @installer.offset_key_pairs_table
        @workers_table           = @installer.workers_table

        # Cache of registered consumer groups for eager offset creation in append.
        # Populated by register_consumer_group.
        # { group_id => { cg_id: Integer, partition_by: Array<String> | nil } }
        @registered_groups = {}
      end

      # Whether all required tables exist.
      # @return [Boolean]
      def installed?
        installer.installed?
      end

      # Create all required tables and indexes. Idempotent.
      # @return [void]
      def install!
        installer.install
      end

      # Drop all tables. Test-only guard.
      # @return [void]
      def uninstall
        installer.uninstall
      end

      # Render the migration to a file for use with the host app's Sequel::Migrator.
      # @see Installer#copy_migration_to
      def copy_migration_to(dir = nil, &block)
        installer.copy_migration_to(dir, &block)
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

            # insert returns last_insert_rowid on SQLite — no need for a separate SELECT
            last_position = db[@messages_table].insert(
              message_id: msg.id,
              message_type: msg.type,
              causation_id: msg.causation_id,
              correlation_id: msg.correlation_id,
              payload: payload_json,
              metadata: metadata_json,
              created_at: msg.created_at.iso8601
            )

            # Upsert key pairs and link to message in 2 statements (was 3):
            # 1. INSERT OR IGNORE the key_pair
            # 2. INSERT message_key_pair with key_pair_id resolved via subquery
            msg.extracted_keys.each do |name, value|
              db.run("INSERT OR IGNORE INTO #{@key_pairs_table} (name, value) VALUES (#{db.literal(name)}, #{db.literal(value)})")
              db.run(<<~SQL)
                INSERT INTO #{@message_key_pairs_table} (message_position, key_pair_id)
                SELECT #{db.literal(last_position)}, id
                FROM #{@key_pairs_table}
                WHERE name = #{db.literal(name)} AND value = #{db.literal(value)}
              SQL
            end
          end

          ensure_offsets_for_registered_groups(messages)
        end

        Console.info "AAA append #{messages.map(&:type).uniq}", messages: messages.size
        notifier.notify_new_messages(messages.map(&:type).uniq)

        last_position
      end

      # Persist messages for future promotion into the main CCC log.
      #
      # @param messages [CCC::Message, Array<CCC::Message>] one or more delayed messages
      # @param at [Time] when the messages should become available
      # @return [Boolean] false when no messages were provided, true otherwise
      def schedule_messages(messages, at:)
        messages = Array(messages)
        return false if messages.empty?

        now = Time.now
        rows = messages.map do |message|
          data = message.to_h
          data[:metadata] = message.metadata.merge(scheduled_at: now)
          {
            created_at: now.iso8601,
            available_at: at.iso8601,
            message: JSON.dump(data)
          }
        end

        db.transaction do
          db[@scheduled_messages_table].multi_insert(rows)
        end

        true
      end

      # Promote due scheduled messages into the main CCC log.
      #
      # Appended messages are re-inserted through {#append} so they are indexed,
      # assigned fresh positions, and announced through the store notifier.
      #
      # @return [Integer] number of scheduled messages promoted
      def update_schedule!
        now = Time.now

        db.transaction do
          rows = db[@scheduled_messages_table]
            .where { available_at <= now.iso8601 }
            .order(:id)
            .limit(100)
            .all

          return 0 if rows.empty?

          messages = rows.map do |row|
            data = JSON.parse(row[:message], symbolize_names: true)
            data[:created_at] = now
            Message.from(data)
          end

          append(messages)

          row_ids = rows.map { |row| row[:id] }
          db[@scheduled_messages_table].where(id: row_ids).delete

          rows.size
        end
      end

      # Paginate the global event log in position order.
      #
      # @example First page
      #   messages = store.read_all(limit: 20)
      #
      # @example Next page (using the last position from the previous page)
      #   messages = store.read_all(from_position: 20, limit: 20)
      #
      # @param from_position [Integer] return messages after this position (default 0)
      # @param limit [Integer] max number of messages to return (default 50)
      # @return [ReadAllResult] messages and last global position
      def read_all(from_position: nil, limit: 50, order: :asc)
        desc = order == :desc
        ds = db[@messages_table]

        if from_position
          ds = desc ? ds.where { position < from_position } : ds.where { position > from_position }
        end

        messages = ds.order(desc ? Sequel.desc(:position) : :position)
          .limit(limit)
          .map { |row| deserialize(row) }

        fetcher = ->(pos) { read_all(from_position: pos, limit: limit, order: order) }
        ReadAllResult.new(messages: messages, last_position: latest_position, fetcher: fetcher)
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
          guard = ConsistencyGuard.new(conditions:, last_position: from_position || latest_position)
          return ReadResult.new(messages: [], guard:)
        end

        messages = query_messages(conditions, from_position:, limit:)
        last_position = messages.any? ? messages.last.position : (from_position || latest_position)
        guard = ConsistencyGuard.new(conditions:, last_position:)
        ReadResult.new(messages:, guard:)
      end

      # Read messages for a specific partition using AND semantics.
      # A message is included only when every partition attribute it declares
      # matches the given value. Messages that don't declare a partition
      # attribute pass through (same logic as {#claim_next}).
      #
      # @example Single partition attribute
      #   result = store.read_partition(
      #     { device_id: 'dev-1' },
      #     handled_types: ['device.registered', 'device.bound']
      #   )
      #   result.messages  # => [#<PositionedMessage ...>, ...]
      #   result.guard     # => #<ConsistencyGuard ...>
      #
      # @example Composite partition (AND semantics — messages must match all attributes they declare)
      #   result = store.read_partition(
      #     { course_name: 'Algebra', user_id: 'joe' },
      #     handled_types: ['course.created', 'user.joined_course']
      #   )
      #   # Returns CourseCreated(course_name: 'Algebra') — matches on its only attribute
      #   # Returns UserJoinedCourse(course_name: 'Algebra', user_id: 'joe') — matches both
      #   # Excludes UserJoinedCourse(course_name: 'Algebra', user_id: 'jane') — user_id mismatch
      #
      # @example Resuming from a position (e.g. after processing a batch)
      #   result = store.read_partition(
      #     { device_id: 'dev-1' },
      #     handled_types: ['device.registered'],
      #     from_position: 42
      #   )
      #   # Only returns messages with position > 42
      #
      # @example Using the guard for optimistic concurrency on append
      #   result = store.read_partition(
      #     { device_id: 'dev-1' },
      #     handled_types: ['device.registered', 'device.bound']
      #   )
      #   # ... build new events from result.messages ...
      #   store.append(new_events, guard: result.guard)
      #   # Raises Sourced::ConcurrentAppendError if conflicting writes occurred
      #
      # @param partition_attrs [Hash{Symbol|String => String}] partition attribute values
      # @param handled_types [Array<String>] message type strings to include
      # @param from_position [Integer] fetch messages after this position (default 0)
      # @return [ReadResult] messages and a guard for optimistic concurrency
      def read_partition(partition_attrs, handled_types:, from_position: 0)
        # Resolve key_pair_ids for each partition attribute
        key_pair_ids = partition_attrs.filter_map do |name, value|
          db[@key_pairs_table].where(name: name.to_s, value: value.to_s).get(:id)
        end

        # If any key pair doesn't exist in the store, no messages can match
        if key_pair_ids.size < partition_attrs.size
          guard = ConsistencyGuard.new(conditions: [], last_position: from_position)
          return ReadResult.new(messages: [], guard:)
        end

        messages = fetch_partition_messages(key_pair_ids, from_position, handled_types)

        # Build guard conditions from handled_types, scoped to partition attrs.
        # These use OR semantics so the guard detects any concurrent write
        # in the broader partition context (e.g. another student enrolling).
        partition_sym = partition_attrs.transform_keys(&:to_sym)
        guard_conditions = handled_types.filter_map do |type|
          klass = Message.registry[type]
          klass&.to_conditions(**partition_sym)
        end.flatten

        # The guard's last_position must cover the full OR-context, not just
        # the AND-filtered messages. Otherwise a message that passes the OR
        # conditions but was excluded by AND filtering would look like a conflict.
        last_pos = max_position_for(guard_conditions, from_position: from_position)

        guard = ConsistencyGuard.new(conditions: guard_conditions, last_position: last_pos)
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
      # When +partition_by+ is provided, offsets are created eagerly during {#append}
      # instead of lazily via discovery in {#claim_next}.
      #
      # @param group_id [String] unique identifier for the consumer group
      # @param partition_by [Array<String, Symbol>, nil] attribute names defining partitions
      # @return [void]
      def register_consumer_group(group_id, partition_by: nil)
        partition_by_sorted = partition_by ? Array(partition_by).map(&:to_s).sort : nil
        partition_by_json = partition_by_sorted ? JSON.dump(partition_by_sorted) : nil
        now = Time.now.iso8601
        db.run(<<~SQL)
          INSERT INTO #{@consumer_groups_table} (group_id, status, highest_position, partition_by, created_at, updated_at)
          VALUES (#{db.literal(group_id)}, '#{ACTIVE}', 0, #{db.literal(partition_by_json)}, #{db.literal(now)}, #{db.literal(now)})
          ON CONFLICT(group_id) DO UPDATE SET partition_by = #{db.literal(partition_by_json)}, updated_at = #{db.literal(now)}
        SQL

        # Cache for hot-path use in append
        cg = db[@consumer_groups_table].where(group_id: group_id).first
        @registered_groups[group_id] = { cg_id: cg[:id], partition_by: partition_by_sorted }
      end

      # Whether the consumer group exists and is active.
      #
      # @param group_id [String, #group_id] identifier or object responding to +#group_id+
      # @return [Boolean]
      def consumer_group_active?(group_id)
        group_id = resolve_group_id(group_id)
        row = db[@consumer_groups_table].where(group_id: group_id).select(:status).first
        return false unless row

        row[:status] == ACTIVE
      end

      # Stop a consumer group intentionally. Stopped groups are skipped by {#claim_next}.
      #
      # @param group_id [String, #group_id] identifier or object responding to +#group_id+
      # @param message [String, nil] optional operator-supplied reason
      # @return [void]
      def stop_consumer_group(group_id, message = nil)
        group_id = resolve_group_id(group_id)
        updating_consumer_group(group_id) do |group|
          group.stop(message:)
        end
      end

      # Re-activate a stopped or failed consumer group, clearing retry state.
      #
      # @param group_id [String, #group_id] identifier or object responding to +#group_id+
      # @return [void]
      def start_consumer_group(group_id)
        group_id = resolve_group_id(group_id)
        db[@consumer_groups_table]
          .where(group_id: group_id)
          .update(status: ACTIVE, retry_at: nil, error_context: nil, updated_at: Time.now.iso8601)
        notifier.notify_reactor_resumed(group_id)
      end

      # Load a consumer group row, yield a {GroupUpdater} for mutation,
      # then persist the accumulated updates atomically.
      # Mirrors SequelBackend#updating_consumer_group.
      #
      # @param group_id [String]
      # @yieldparam group [CCC::GroupUpdater]
      # @return [void]
      def updating_consumer_group(group_id)
        dataset = db[@consumer_groups_table].where(group_id: group_id)
        row = dataset.first
        raise ArgumentError, "Consumer group #{group_id} not found" unless row

        ctx = row[:error_context] ? JSON.parse(row[:error_context], symbolize_names: true) : {}
        row[:error_context] = ctx

        group = CCC::GroupUpdater.new(group_id, row, logger)
        yield group

        updates = group.updates.dup
        updates[:error_context] = JSON.dump(updates[:error_context])
        dataset.update(updates)
      end

      # Delete all offsets for a consumer group, resetting it to process from the beginning.
      #
      # @param group_id [String, #group_id] identifier or object responding to +#group_id+
      # @return [void]
      def reset_consumer_group(group_id)
        group_id = resolve_group_id(group_id)
        cg = db[@consumer_groups_table].where(group_id: group_id).first
        return unless cg

        db[@offsets_table].where(consumer_group_id: cg[:id]).delete
        db[@consumer_groups_table].where(id: cg[:id]).update(
          discovery_position: 0,
          last_nil_types_max_pos: 0,
          updated_at: Time.now.iso8601
        )
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
      # @param batch_size [Integer, nil] max messages to fetch per claim (nil = unlimited)
      # @return [Hash, nil] +{ offset_id:, key_pair_ids:, partition_key:, partition_value:, messages:, replaying:, guard: }+ or nil
      def claim_next(group_id, partition_by:, handled_types:, worker_id:, batch_size: nil)
        partition_by = Array(partition_by).sort
        now = Time.now.iso8601
        cg = db[@consumer_groups_table]
          .where(group_id: group_id, status: ACTIVE)
          .where { Sequel.|({retry_at: nil}, Sequel.lit('retry_at <= ?', now)) }
          .first
        return nil unless cg

        # Short-circuit: no new messages since the last nil claim.
        types_max_pos = db[@messages_table]
          .where(message_type: handled_types)
          .max(:position) || 0

        return nil if types_max_pos <= cg[:last_nil_types_max_pos]

        claimed = nil
        group_info = @registered_groups[group_id]

        if group_info&.fetch(:partition_by, nil)
          # Eager path: offsets were created by append. Try fast claim first,
          # fall back to discovery only for catch-up (new group against existing log).
          claimed = find_and_claim_partition(cg[:id], handled_types, worker_id)
          unless claimed
            discover_new_partitions(cg[:id], partition_by, handled_types)
            claimed = find_and_claim_partition(cg[:id], handled_types, worker_id)
          end
        else
          # Legacy path: lazy discovery
          has_offsets = db[@offsets_table].where(consumer_group_id: cg[:id]).limit(1).any?
          if has_offsets && types_max_pos <= cg[:discovery_position]
            claimed = find_and_claim_partition(cg[:id], handled_types, worker_id)
          end
          unless claimed
            discover_new_partitions(cg[:id], partition_by, handled_types)
            claimed = find_and_claim_partition(cg[:id], handled_types, worker_id)
          end
        end

        unless claimed
          # Remember types_max_pos so next poll short-circuits instantly
          db[@consumer_groups_table].where(id: cg[:id])
            .update(last_nil_types_max_pos: types_max_pos)
          return nil
        end

        key_pair_ids = db[@offset_key_pairs_table]
          .where(offset_id: claimed[:offset_id])
          .select_map(:key_pair_id)

        messages = fetch_partition_messages(key_pair_ids, claimed[:last_position], handled_types, limit: batch_size)

        # If no messages pass the conditional AND filter, release and return nil
        if messages.empty?
          release(group_id, offset_id: claimed[:offset_id])
          return nil
        end

        # Build partition_value hash from key_pairs
        partition_value = {}
        db[@key_pairs_table].where(id: key_pair_ids).each do |kp|
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
        cg = db[@consumer_groups_table].where(group_id: group_id).first
        return unless cg

        db[@offsets_table].where(id: offset_id, consumer_group_id: cg[:id]).update(
          last_position: position,
          claimed: 0,
          claimed_at: nil,
          claimed_by: nil
        )

        # Advance the high watermark (never decrease)
        if position > cg[:highest_position]
          db[@consumer_groups_table].where(id: cg[:id]).update(
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
        cg = db[@consumer_groups_table].where(group_id: group_id).first
        return unless cg

        db[@offsets_table].where(id: offset_id, consumer_group_id: cg[:id]).update(
          claimed: 0,
          claimed_at: nil,
          claimed_by: nil
        )
      end

      # Upsert heartbeat timestamps for active workers.
      #
      # @param worker_ids [Array<String>] worker identifiers
      # @param at [Time] timestamp to record (default Time.now)
      # @return [Integer] number of workers heartbeated
      def worker_heartbeat(worker_ids, at: Time.now)
        ids = Array(worker_ids).uniq
        return 0 if ids.empty?

        now = at.iso8601
        ids.each do |id|
          db.run(<<~SQL)
            INSERT INTO #{@workers_table} (id, last_seen) VALUES (#{db.literal(id)}, #{db.literal(now)})
            ON CONFLICT(id) DO UPDATE SET last_seen = #{db.literal(now)}
          SQL
        end
        ids.size
      end

      # Release claims held by workers that haven't heartbeated within ttl_seconds.
      #
      # @param ttl_seconds [Integer] age threshold
      # @return [Integer] number of claims released
      def release_stale_claims(ttl_seconds: 120)
        cutoff = (Time.now - ttl_seconds).iso8601

        stale_worker_ids = db[@workers_table]
          .where(Sequel.lit('last_seen <= ?', cutoff))
          .select_map(:id)

        return 0 if stale_worker_ids.empty?

        db[@offsets_table]
          .where(claimed: 1)
          .where(claimed_by: stale_worker_ids)
          .update(claimed: 0, claimed_at: nil, claimed_by: nil)
      end

      # Advance a consumer group's offset for a specific partition to at least +position+.
      # Bootstraps the offset row if it doesn't exist yet.
      # Unlike {#ack}, this does not require a prior claim.
      #
      # @param group_id [String] consumer group identifier
      # @param partition [Hash{String => String}] partition attribute names and values
      # @param position [Integer] advance offset to at least this position
      # @return [void]
      def advance_offset(group_id, partition:, position:)
        cg = db[@consumer_groups_table].where(group_id: group_id).first
        return unless cg

        offset_id = ensure_offset_for_partition(cg[:id], partition)
        return unless offset_id

        offset = db[@offsets_table].where(id: offset_id).first
        return if offset[:last_position] >= position

        db[@offsets_table].where(id: offset_id).update(last_position: position)

        if position > cg[:highest_position]
          db[@consumer_groups_table].where(id: cg[:id]).update(
            highest_position: position,
            updated_at: Time.now.iso8601
          )
        end
      end

      # System-wide diagnostics for monitoring and debugging.
      #
      # @example
      #   stats = store.stats
      #   stats.max_position  # => 42
      #   stats.groups
      #   # => [
      #   #   {
      #   #     group_id: "my_decider",
      #   #     status: "active",
      #   #     retry_at: nil,
      #   #     error_context: {},
      #   #     oldest_processed: 10,
      #   #     newest_processed: 42,
      #   #     partition_count: 3
      #   #   },
      #   #   {
      #   #     group_id: "failing_decider",
      #   #     status: "failed",
      #   #     retry_at: nil,
      #   #     error_context: { exception_class: "RuntimeError", exception_message: "boom" },
      #   #     oldest_processed: 5,
      #   #     newest_processed: 30,
      #   #     partition_count: 2
      #   #   }
      #   # ]
      #
      # @return [CCC::Stats] max_position and per-group processing state
      def stats
        groups = db.fetch(<<~SQL).all
          SELECT
            cg.group_id,
            cg.status,
            cg.retry_at,
            cg.error_context,
            COALESCE(MIN(CASE WHEN o.last_position > 0 THEN o.last_position END), 0) AS oldest_processed,
            COALESCE(MAX(o.last_position), 0) AS newest_processed,
            COUNT(o.id) AS partition_count
          FROM #{@consumer_groups_table} cg
          LEFT JOIN #{@offsets_table} o ON o.consumer_group_id = cg.id
          GROUP BY cg.id, cg.group_id, cg.status, cg.retry_at, cg.error_context
          ORDER BY cg.group_id
        SQL

        groups.each do |g|
          g[:retry_at] = Time.parse(g[:retry_at]) if g[:retry_at]
          g[:error_context] = g[:error_context] ? JSON.parse(g[:error_context], symbolize_names: true) : {}
        end

        Stats.new(max_position: latest_position, groups: groups)
      end

      # Fetch all messages sharing the same correlation_id as the given message.
      # Useful for tracing causal chains (command -> events -> reactions).
      #
      # @param message_id [String] UUID of any message in the correlation chain
      # @return [Array<PositionedMessage>] correlated messages ordered by position, or [] if not found
      def read_correlation_batch(message_id)
        correlation_id = db[@messages_table]
          .where(message_id: message_id)
          .get(:correlation_id)
        return [] unless correlation_id

        db[@messages_table]
          .where(correlation_id: correlation_id)
          .order(:position)
          .map { |row| deserialize(row) }
      end

      # Current max position in the message log.
      #
      # @return [Integer] max position, or 0 if the store is empty
      def latest_position
        db[@messages_table].max(:position) || 0
      end

      # Delete all data from all tables and reset autoincrement. For testing only.
      #
      # @return [void]
      def clear!
        db[@offset_key_pairs_table].delete
        db[@offsets_table].delete
        db[@consumer_groups_table].delete
        db[@message_key_pairs_table].delete
        db[@key_pairs_table].delete
        db[@messages_table].delete
        db[@scheduled_messages_table].delete
        db[@workers_table].delete
        db.run('DELETE FROM sqlite_sequence') if db.table_exists?(:sqlite_sequence)
      end

      private

      # Resolve a group_id argument that is either a String
      # or an object responding to +#group_id+.
      #
      # @param group_id [String, #group_id]
      # @return [String]
      def resolve_group_id(group_id)
        group_id.respond_to?(:group_id) ? group_id.group_id : group_id
      end

      # Create offsets eagerly for all registered consumer groups.
      # Called inside the append transaction after messages and key_pairs are inserted.
      #
      # @param messages [Array<CCC::Message>] messages being appended
      # @return [void]
      def ensure_offsets_for_registered_groups(messages)
        return if @registered_groups.empty?

        # Collect all partition attribute names across registered groups
        attr_names = @registered_groups.each_value.flat_map { |gi| gi[:partition_by] || [] }.uniq

        # Pre-fetch relevant key_pair IDs in one query, keyed by "name:value"
        kp_id_cache = {}
        db[@key_pairs_table].where(name: attr_names).each do |row|
          kp_id_cache["#{row[:name]}:#{row[:value]}"] = row[:id]
        end

        @registered_groups.each_value do |group_info|
          partition_by = group_info[:partition_by]
          next unless partition_by

          cg_id = group_info[:cg_id]
          seen = Set.new

          messages.each do |msg|
            keys = msg.extracted_keys.to_h  # {"device_id"=>"dev-1", "name"=>"A"}
            next unless partition_by.all? { |attr| keys.key?(attr) }

            values = partition_by.to_h { |attr| [attr, keys[attr]] }
            pk = build_partition_key(partition_by, values)
            next if seen.include?(pk)
            seen << pk

            kp_ids = partition_by.map { |attr| kp_id_cache["#{attr}:#{values[attr]}"] }
            create_offset_with_key_pairs(cg_id, partition_by, values, kp_ids)
          end
        end
      end

      # Build canonical partition key string from attribute names and values.
      # Sorted by attribute name for deterministic uniqueness.
      #
      # @param partition_by [Array<String>] attribute names
      # @param values [Hash{String => String}] attribute values keyed by name
      # @return [String] e.g. "course_name:Algebra|user_id:joe"
      def build_partition_key(partition_by, values)
        partition_by.sort.map { |attr| "#{attr}:#{values[attr]}" }.join('|')
      end

      # Scan a bounded window of messages forward from the consumer group's
      # discovery_position watermark, find new partition tuples, create offsets
      # for them, then advance the watermark.
      #
      # @param cg_id [Integer] consumer group internal ID
      # @param partition_by [Array<String>] sorted attribute names
      # @param handled_types [Array<String>] message type strings
      # @return [void]
      def discover_new_partitions(cg_id, partition_by, handled_types)
        cg = db[@consumer_groups_table].where(id: cg_id).first
        discovery_pos = cg[:discovery_position]

        types_list = handled_types.map { |t| db.literal(t) }.join(', ')

        # CTE per partition attribute: pre-joins message_key_pairs with key_pairs
        # so the main query only self-joins on the CTEs (N joins instead of 2N).
        ctes = []
        selects = []
        joins = []
        partition_by.each_with_index do |attr, i|
          ctes << <<~CTE
            attr#{i} AS (
              SELECT mkp.message_position, kp.id AS kp_id, kp.value AS val
              FROM #{@message_key_pairs_table} mkp
              JOIN #{@key_pairs_table} kp ON mkp.key_pair_id = kp.id AND kp.name = #{db.literal(attr)}
            )
          CTE
          selects << "a#{i}.kp_id AS kp_id_#{i}, a#{i}.val AS val_#{i}"
          joins << "JOIN attr#{i} a#{i} ON m.position = a#{i}.message_position"
        end

        group_by = partition_by.each_index.map { |i| "a#{i}.kp_id" }.join(', ')

        # Discover all partition tuples in the window (no NOT EXISTS — fast).
        # Duplicates are filtered in Ruby against known partition_keys, and
        # INSERT OR IGNORE handles any remaining races.
        sql = <<~SQL
          WITH #{ctes.join(",\n")}
          SELECT #{selects.join(', ')},
                 MIN(m.position) AS min_pos,
                 MAX(m.position) AS max_pos
          FROM #{@messages_table} m
          #{joins.join("\n")}
          WHERE m.message_type IN (#{types_list})
            AND m.position > #{db.literal(discovery_pos)}
          GROUP BY #{group_by}
          ORDER BY min_pos ASC
          LIMIT #{DISCOVERY_BATCH_SIZE}
        SQL

        rows = db.fetch(sql).all

        max_discovered_pos = 0

        if rows.any?
          db.transaction do
            rows.each do |row|
              values = {}
              kp_ids = []
              partition_by.each_with_index do |attr, i|
                values[attr] = row[:"val_#{i}"]
                kp_ids << row[:"kp_id_#{i}"]
              end

              # INSERT OR IGNORE handles duplicates — no need to pre-filter.
              # At most DISCOVERY_BATCH_SIZE (100) tuples per call, so the
              # cost of re-attempting known offsets is bounded.
              create_offset_with_key_pairs(cg_id, partition_by, values, kp_ids)
              max_discovered_pos = row[:max_pos] if row[:max_pos] > max_discovered_pos
            end
          end
        end

        # Advance watermark to the max position seen in the window (whether new or known).
        # This ensures we don't re-scan these messages on the next discovery call.
        max_window_pos = rows.any? ? rows.map { |r| r[:max_pos] }.max : 0
        new_watermark = [max_discovered_pos, max_window_pos, latest_position].select { |p| p > 0 }.min || discovery_pos
        # If we found a full batch, advance only to the batch's max (more may follow).
        # If we found fewer than a batch, we've scanned to the end — advance to latest.
        new_watermark = latest_position if rows.size < DISCOVERY_BATCH_SIZE

        if new_watermark > discovery_pos
          db[@consumer_groups_table].where(id: cg_id).update(
            discovery_position: new_watermark,
            updated_at: Time.now.iso8601
          )
        end
      end

      # Ensure an offset row exists for a specific partition (by attribute values).
      # Resolves key_pair IDs from the key_pairs table; returns nil if any
      # partition attribute has no corresponding key_pair (meaning no messages
      # with that attribute value exist yet).
      #
      # @param cg_id [Integer] consumer group internal ID
      # @param partition [Hash{String => String}] attribute names and values
      # @return [Integer, nil] offset ID, or nil if key_pairs not found
      def ensure_offset_for_partition(cg_id, partition)
        partition_by = partition.keys.sort
        kp_ids = []
        partition_by.each do |attr|
          kp = db[@key_pairs_table].where(name: attr, value: partition[attr].to_s).first
          return nil unless kp

          kp_ids << kp[:id]
        end

        create_offset_with_key_pairs(cg_id, partition_by, partition, kp_ids)
      end

      # Create an offset row and its key_pair associations. Idempotent via INSERT OR IGNORE.
      #
      # @param cg_id [Integer] consumer group internal ID
      # @param partition_by [Array<String>] sorted attribute names
      # @param values [Hash{String => String}] attribute values keyed by name
      # @param kp_ids [Array<Integer>] key_pair IDs
      # @return [Integer] offset ID
      def create_offset_with_key_pairs(cg_id, partition_by, values, kp_ids)
        partition_key = build_partition_key(partition_by, values)

        db.run(<<~SQL)
          INSERT OR IGNORE INTO #{@offsets_table} (consumer_group_id, partition_key, last_position, claimed)
          VALUES (#{db.literal(cg_id)}, #{db.literal(partition_key)}, 0, 0)
        SQL

        offset_id = db[@offsets_table].where(consumer_group_id: cg_id, partition_key: partition_key).get(:id)

        kp_ids.each do |kp_id|
          db.run(<<~SQL)
            INSERT OR IGNORE INTO #{@offset_key_pairs_table} (offset_id, key_pair_id)
            VALUES (#{db.literal(offset_id)}, #{db.literal(kp_id)})
          SQL
        end

        offset_id
      end

      # Find the next unclaimed partition with pending messages and claim it.
      # Uses OR joins for candidate detection (any matching key_pair), then
      # a NOT EXISTS clause to exclude messages that conflict on a shared
      # attribute name (same name, different value). This gives AND semantics:
      # a message only counts as pending for a partition when every attribute
      # it shares with the partition has the same value.
      #
      # @param cg_id [Integer] consumer group internal ID
      # @param handled_types [Array<String>] message type strings
      # @param worker_id [String] claiming worker identifier
      # @return [Hash, nil] +{ offset_id:, partition_key:, last_position: }+ or nil
      def find_and_claim_partition(cg_id, handled_types, worker_id)
        types_list = handled_types.map { |t| db.literal(t) }.join(', ')

        row = nil
        db[@offsets_table]
          .where(consumer_group_id: cg_id, claimed: 0)
          .select(:id, :partition_key, :last_position)
          .order(:last_position)
          .each do |offset|

          pending = db.fetch(<<~SQL).first
            SELECT 1
            FROM #{@offset_key_pairs_table} okp
            JOIN #{@message_key_pairs_table} mkp ON okp.key_pair_id = mkp.key_pair_id
            JOIN #{@messages_table} m ON mkp.message_position = m.position
            WHERE okp.offset_id = #{db.literal(offset[:id])}
              AND m.position > #{db.literal(offset[:last_position])}
              AND m.message_type IN (#{types_list})
            GROUP BY m.position
            HAVING COUNT(*) = (
              SELECT COUNT(*) FROM #{@offset_key_pairs_table} WHERE offset_id = #{db.literal(offset[:id])}
            )
            LIMIT 1
          SQL

          if pending
            row = { offset_id: offset[:id], partition_key: offset[:partition_key], last_position: offset[:last_position] }
            break
          end
        end

        return nil unless row

        now = Time.now.iso8601
        updated = db[@offsets_table]
          .where(id: row[:offset_id], claimed: 0)
          .update(claimed: 1, claimed_at: now, claimed_by: worker_id)

        return nil if updated == 0

        row
      end

      # Fetch messages for a partition using conditional AND semantics.
      # For each candidate message, it must match ALL of the partition's attributes
      # that the message itself has. Messages with a single partition attribute match
      # on that one; messages with multiple must match all of them.
      #
      # @param key_pair_ids [Array<Integer>] partition key_pair IDs
      # @param last_position [Integer] fetch messages after this position
      # @param handled_types [Array<String>] message type strings
      # @param limit [Integer, nil] max messages to return (nil = unlimited)
      # @return [Array<PositionedMessage>]
      def fetch_partition_messages(key_pair_ids, last_position, handled_types, limit: nil)
        return [] if key_pair_ids.empty?

        kp_ids_list = key_pair_ids.map { |id| db.literal(id) }.join(', ')
        types_list = handled_types.map { |t| db.literal(t) }.join(', ')

        # CTE pre-resolves which attribute names the partition's key_pairs cover.
        # The main query uses this to count-match: a message qualifies when it
        # matches as many partition key_pairs as it has attributes in common with
        # the partition (AND semantics for shared attributes).
        sql = <<~SQL
          WITH partition_attr_names AS (
            SELECT DISTINCT name FROM #{@key_pairs_table} WHERE id IN (#{kp_ids_list})
          )
          SELECT DISTINCT m.position, m.message_id, m.message_type, m.causation_id, m.correlation_id, m.payload, m.metadata, m.created_at
          FROM #{@messages_table} m
          WHERE m.position > #{db.literal(last_position)}
            AND m.message_type IN (#{types_list})
            AND EXISTS (
              SELECT 1 FROM #{@message_key_pairs_table} mkp
              WHERE mkp.message_position = m.position
                AND mkp.key_pair_id IN (#{kp_ids_list})
            )
            AND (
              SELECT COUNT(*) FROM #{@message_key_pairs_table} mkp
              WHERE mkp.message_position = m.position
                AND mkp.key_pair_id IN (#{kp_ids_list})
            ) = (
              SELECT COUNT(DISTINCT kp_msg.name)
              FROM #{@message_key_pairs_table} mkp2
              JOIN #{@key_pairs_table} kp_msg ON mkp2.key_pair_id = kp_msg.id
              WHERE mkp2.message_position = m.position
                AND kp_msg.name IN (SELECT name FROM partition_attr_names)
            )
          ORDER BY m.position ASC
        SQL
        sql += " LIMIT #{db.literal(limit)}" if limit

        db.fetch(sql).map { |row| deserialize(row) }
      end

      # Core query logic shared by {#read} and {#check_conflicts}.
      # Resolves key_pair IDs from conditions, then queries messages.
      # Attributes within each condition are AND'd; conditions are OR'd.
      #
      # @param conditions [Array<QueryCondition>]
      # @param from_position [Integer, nil]
      # @param limit [Integer, nil]
      # @return [Array<PositionedMessage>]
      def query_messages(conditions, from_position: nil, limit: nil)
        subqueries = condition_position_subqueries(conditions, from_position: from_position)
        return [] if subqueries.empty?

        union = subqueries.join(" UNION ")

        sql = <<~SQL
          SELECT m.position, m.message_id, m.message_type, m.causation_id, m.correlation_id, m.payload, m.metadata, m.created_at
          FROM #{@messages_table} m
          WHERE m.position IN (#{union})
          ORDER BY m.position
        SQL
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

      # Max position among messages matching the given conditions.
      # Attributes within each condition are AND'd; conditions are OR'd.
      # Returns from_position (or latest_position) if no matches.
      #
      # @param conditions [Array<QueryCondition>]
      # @param from_position [Integer, nil]
      # @return [Integer]
      def max_position_for(conditions, from_position: nil)
        return from_position || latest_position if conditions.empty?

        subqueries = condition_position_subqueries(conditions, from_position: from_position)
        return from_position || latest_position if subqueries.empty?

        union = subqueries.join(" UNION ")
        row = db.fetch("SELECT MAX(position) AS max_pos FROM (#{union})").first
        row[:max_pos] || from_position || latest_position
      end

      # Build per-condition position subqueries with AND-within/OR-across semantics.
      # Resolves key_pair IDs, then builds one SQL subquery per condition.
      # Each subquery selects positions where the message matches ALL attrs in the condition.
      #
      # @param conditions [Array<QueryCondition>]
      # @param from_position [Integer, nil] only include positions after this
      # @return [Array<String>] SQL subquery strings (empty if no conditions can match)
      def condition_position_subqueries(conditions, from_position: nil)
        all_lookups = conditions.flat_map { |c| c.attrs.map { |k, v| [k.to_s, v.to_s] } }.uniq
        return [] if all_lookups.empty?

        or_clauses = all_lookups.map { |n, v| "(name = #{db.literal(n)} AND value = #{db.literal(v)})" }
        key_rows = db.fetch("SELECT id, name, value FROM #{@key_pairs_table} WHERE #{or_clauses.join(' OR ')}").all

        key_pair_index = {}
        key_rows.each { |r| key_pair_index[[r[:name], r[:value]]] = r[:id] }

        position_filter = from_position ? "AND m.position > #{db.literal(from_position)}" : ""

        conditions.filter_map do |c|
          kp_ids = c.attrs.filter_map { |k, v| key_pair_index[[k.to_s, v.to_s]] }
          next if kp_ids.size < c.attrs.size

          kp_ids_list = kp_ids.map { |id| db.literal(id) }.join(', ')

          <<~SQL
            SELECT m.position
            FROM #{@messages_table} m
            JOIN #{@message_key_pairs_table} mkp ON m.position = mkp.message_position
            WHERE m.message_type = #{db.literal(c.message_type)}
              AND mkp.key_pair_id IN (#{kp_ids_list})
              #{position_filter}
            GROUP BY m.position
            HAVING COUNT(DISTINCT mkp.key_pair_id) = #{kp_ids.size}
          SQL
        end
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
