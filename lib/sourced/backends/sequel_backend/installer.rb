# frozen_string_literal: true

require 'sequel'
module Sourced
  module Backends
    class SequelBackend
      class Installer
        def initialize(
          db,
          logger:,
          workers_table:,
          scheduled_messages_table:,
          streams_table:,
          offsets_table:,
          consumer_groups_table:,
          messages_table:
        )
          @db = db
          @logger = logger
          @scheduled_messages_table = scheduled_messages_table
          @workers_table = workers_table
          @streams_table = streams_table
          @offsets_table = offsets_table
          @consumer_groups_table = consumer_groups_table
          @messages_table = messages_table
        end

        def installed?
          db.table_exists?(messages_table) \
            && db.table_exists?(streams_table) \
            && db.table_exists?(consumer_groups_table) \
            && db.table_exists?(offsets_table) \
            && db.table_exists?(scheduled_messages_table) \
            && db.table_exists?(workers_table)
        end

        def uninstall
          return unless installed?

          raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

          [offsets_table, scheduled_messages_table, messages_table, consumer_groups_table, streams_table, workers_table].each do |table|
            db.drop_table?(table)
          end
        end

        def install
          _streams_table = streams_table
          _consumer_groups_table = consumer_groups_table

          db.create_table?(streams_table) do
            primary_key :id
            String :stream_id, null: false, unique: true
            Time :updated_at, null: false, default: Sequel.function(:now)
            Bignum :seq, null: false
            
            # Index for recent_streams query performance
            index :updated_at, name: "idx_#{_streams_table}_updated_at"
          end

          logger.info("Created table #{streams_table}")

          db.create_table?(consumer_groups_table) do
            primary_key :id
            String :group_id, null: false, unique: true
            Bignum :highest_global_seq, null: false, default: 0
            String :status, null: false, default: ACTIVE, index: true
            column :error_context, :jsonb
            Time :retry_at, null: true, index: true
            Time :created_at, null: false, default: Sequel.function(:now)
            Time :updated_at, null: false, default: Sequel.function(:now)

            index :group_id, unique: true
          end

          logger.info("Created table #{consumer_groups_table}")

          _offsets_table = offsets_table
          
          db.create_table?(offsets_table) do
            primary_key :id
            foreign_key :group_id, _consumer_groups_table, on_delete: :cascade
            foreign_key :stream_id, _streams_table, on_delete: :cascade
            Bignum :global_seq, null: false
            Time :created_at, null: false, default: Sequel.function(:now)
            TrueClass :claimed, null: false, default: false
            Time :claimed_at, null: true
            String :claimed_by, null: true

            # Unique constraint for business logic
            index %i[group_id stream_id], unique: true
            index :claimed, where: { claimed: false }, name: "idx_#{_offsets_table}_unclaimed"
            index [:claimed, :claimed_by, :claimed_at], where: { claimed: true }, name: "idx_#{_offsets_table}_claimed_claimer"
            
            # Coverage index for aggregation queries (sql_for_consumer_stats)
            # Covers: GROUP BY group_id + MIN/MAX(global_seq) aggregations
            index %i[group_id global_seq], name: "idx_#{_offsets_table}_group_seq_covering"
          end

          logger.info("Created table #{offsets_table}")

          db.create_table?(messages_table) do
            primary_key :global_seq, type: :Bignum
            column :id, :uuid, unique: true
            foreign_key :stream_id, _streams_table
            Bignum :seq, null: false
            String :type, null: false
            Time :created_at, null: false
            column :causation_id, :uuid, index: true
            column :correlation_id, :uuid, index: true
            column :metadata, :jsonb
            column :payload, :jsonb
            
            # Existing indexes
            index %i[stream_id seq], unique: true
            
            # Performance indexes for common query patterns
            index :type                           # For event type filtering
            index :created_at                     # For time-based queries
            index %i[type global_seq]             # For filtered ordering (composite)
            index %i[stream_id global_seq]        # For stream + sequence queries
          end

          logger.info("Created table #{messages_table}")

          _scheduled_messages_table = scheduled_messages_table

          db.create_table?(scheduled_messages_table) do
            primary_key :id
            Time :created_at, null: false
            Time :available_at, null: false
            column :message, :jsonb

            index :available_at
          end

          logger.info("Created table #{scheduled_messages_table}")

          db.create_table?(workers_table) do
            String :id, primary_key: true, null: false
            Time :last_seen, null: false, index: true
            String :pid, null: true
            String :host, null: true
            column :info, :jsonb
          end

          logger.info("Created table #{workers_table}")
        end

        private

        attr_reader(
          :db, 
          :logger, 
          :scheduled_messages_table,
          :workers_table,
          :streams_table,
          :offsets_table,
          :consumer_groups_table,
          :messages_table
        )
      end
    end
  end
end
