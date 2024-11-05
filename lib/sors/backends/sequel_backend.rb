# frozen_string_literal: true

require 'sequel'
require 'json'
require 'sors/message'

Sequel.extension :fiber_concurrency
Sequel.extension :pg_json if defined?(PG)

module Sors
  module Backends
    class SequelBackend
      def initialize(db, logger: Sors.config.logger, prefix: 'sors')
        @db = connect(db)
        @logger = logger
        @prefix = prefix
        @streams_table = table_name(:streams)
        @offsets_table = table_name(:offsets)
        @events_table = table_name(:events)
        logger.info("Connected to #{@db}")
      end

      def installed?
        db.table_exists?(events_table) && db.table_exists?(streams_table) && db.table_exists?(offsets_table)
      end

      def schedule_commands(commands)
        return false if commands.empty?

        # TODO: here we could use multi_insert
        # for both streams and commands
        db.transaction do
          commands.each do |command|
            schedule_command(command.stream_id, command)
          end
        end
        true
      end

      def schedule_command(stream_id, command)
        db.transaction do
          db[streams_table].insert_conflict.insert(stream_id:)
          db[commands_table].insert(stream_id:, data: command.to_json)
        end
      end

      Stats = Data.define(:stream_count, :max_global_seq, :groups)

      def stats
        stream_count = db[streams_table].count
        max_global_seq = db[events_table].max(:global_seq)
        groups = db.fetch(sql_for_consumer_stats).all
        Stats.new(stream_count, max_global_seq, groups)
      end

      def transaction(&)
        db.transaction(&)
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
        raise Sors::ConcurrentAppendError, e.message
      end

      # @param reactor [Sors::ReactorInterface]
      def reserve_next_for_reactor(reactor, &)
        group_id = reactor.consumer_info.group_id
        handled_events = reactor.handled_events.map(&:type)

        db.transaction do
          row = db.fetch(sql_for_reserve_next_with_events(handled_events), group_id, group_id).first
          return unless row

          event = deserialize_event(row)

          if block_given?
            yield(event)
            # ACK
            ack_event(group_id, row[:stream_id_fk], row[:global_seq])
          end

          event
        end
      end

      private def ack_event(group_id, stream_id, global_seq)
        db[offsets_table]
          .insert_conflict(
            target: [:group_id, :stream_id],
            update: { global_seq: Sequel[:excluded][:global_seq] }
          )
          .insert(stream_id:, group_id:, global_seq:)
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
            Sequel[events_table][:producer],
            Sequel[events_table][:causation_id],
            Sequel[events_table][:correlation_id],
            Sequel[events_table][:payload],
          )
          .join(streams_table, id: :stream_id)
      end

      def read_event_batch(causation_id)
        query = base_events_query
          .where(Sequel[events_table][:causation_id] => causation_id)
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
        db[offsets_table].delete
        db[streams_table].delete
      end

      def install
        if @db.class.name == 'Sequel::SQLite::Database'
          raise 'no SQLite support yet'
        end

        _streams_table = streams_table

        db.create_table?(streams_table) do
          primary_key :id
          String :stream_id, null: false, unique: true
          Time :updated_at, null: false, default: Sequel.function(:now)
          Bignum :seq, null: false
        end

        logger.info("Created table #{streams_table}")

        db.create_table?(offsets_table) do
          primary_key :id
          foreign_key :stream_id, _streams_table
          String :group_id, null: false, index: true
          Bignum :global_seq, null: false
          Time :created_at, null: false, default: Sequel.function(:now)
          index %i[group_id stream_id], unique: true
        end

        logger.info("Created table #{offsets_table}")

        db.create_table?(events_table) do
          primary_key :global_seq, type: :Bignum
          column :id, :uuid, unique: true
          foreign_key :stream_id, _streams_table
          Bignum :seq, null: false
          String :type, null: false
          Time :created_at, null: false
          String :producer
          column :causation_id, :uuid, index: true
          column :correlation_id, :uuid
          column :payload, :jsonb
          index %i[stream_id seq], unique: true
        end

        logger.info("Created table #{events_table}")

        self
      end

      private

      attr_reader :db, :logger, :prefix, :events_table, :streams_table, :offsets_table

      def sql_for_reserve_next_with_events(handled_events)
        event_types = handled_events.map { |e| "'#{e}'" }
        event_types_sql = event_types.any? ? " AND e.type IN(#{event_types.join(',')})" : ''

        <<~SQL
          WITH candidate_events AS (
              SELECT 
                  e.global_seq,
                  e.id,
                  e.stream_id AS stream_id_fk,
                  s.stream_id,
                  e.seq,
                  e.type,
                  e.causation_id,
                  e.correlation_id,
                  e.payload,
                  e.created_at,
                  pg_try_advisory_xact_lock(hashtext(?::text), hashtext(s.id::text)) as lock_obtained
              FROM #{events_table} e
              JOIN #{streams_table} s ON e.stream_id = s.id
              LEFT JOIN #{offsets_table} o ON o.stream_id = e.stream_id 
                  AND o.group_id = ?
              WHERE e.global_seq > COALESCE(o.global_seq, 0)#{event_types_sql}
              ORDER BY e.global_seq
          )
          SELECT *
          FROM candidate_events
          WHERE lock_obtained = true
          LIMIT 1;
        SQL
      end

      def sql_for_consumer_stats
        @sql_for_consumer_stats ||= <<~SQL
          SELECT 
              group_id,
              min(global_seq) as oldest_processed,
              max(global_seq) as newest_processed,
              count(*) as stream_count
          FROM #{offsets_table}
          GROUP BY group_id;
        SQL
      end

      def table_name(name)
        [prefix, name].join('_').to_sym
      end

      def parse_json(json)
        return json unless json.is_a?(String)

        JSON.parse(json, symbolize_names: true)
      end

      def serialize_event(event, stream_id)
        row = event.to_h
        row[:stream_id] = stream_id
        row[:payload] = JSON.dump(row[:payload]) if row[:payload]
        row
      end

      def deserialize_event(row)
        row[:payload] = parse_json(row[:payload]) if row[:payload]
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
