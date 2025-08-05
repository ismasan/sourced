# frozen_string_literal: true

require 'sequel'
module Sourced
  module Backends
    class SequelBackend
      class Installer
        def initialize(
          db,
          logger:,
          commands_table:,
          streams_table:,
          offsets_table:,
          consumer_groups_table:,
          events_table:
        )
          @db = db
          @logger = logger
          @commands_table = commands_table
          @streams_table = streams_table
          @offsets_table = offsets_table
          @consumer_groups_table = consumer_groups_table
          @events_table = events_table
        end

        def installed?
          db.table_exists?(events_table) \
            && db.table_exists?(streams_table) \
            && db.table_exists?(consumer_groups_table) \
            && db.table_exists?(offsets_table) \
            && db.table_exists?(commands_table)
        end

        def uninstall
          return unless installed?

          raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

          [offsets_table, commands_table, events_table, consumer_groups_table, streams_table].each do |table|
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

            # Unique constraint for business logic
            index %i[group_id stream_id], unique: true
            index :claimed, where: { claimed: false }, name: "idx_#{_offsets_table}_unclaimed"
            
            # Coverage index for aggregation queries (sql_for_consumer_stats)
            # Covers: GROUP BY group_id + MIN/MAX(global_seq) aggregations
            index %i[group_id global_seq], name: "idx_#{_offsets_table}_group_seq_covering"
          end

          logger.info("Created table #{offsets_table}")

          db.create_table?(events_table) do
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

          logger.info("Created table #{events_table}")

          _commands_table = commands_table
          
          db.create_table?(commands_table) do
            column :id, :uuid, unique: true
            String :consumer_group_id, null: false
            String :stream_id, null: false  
            String :type, null: false
            Time :created_at, null: false
            column :causation_id, :uuid
            column :correlation_id, :uuid
            column :metadata, :jsonb
            column :payload, :jsonb
            
            # Optimized composite index for command processing queries
            # Covers: consumer_group_id lookup + created_at ordering  
            index %i[consumer_group_id created_at], name: "idx_#{_commands_table}_group_created"
            
            # Individual indexes for other access patterns
            index :stream_id     # For stream-specific command queries
            index :type          # For command type filtering
          end

          logger.info("Created table #{commands_table}")
        end

        private

        attr_reader(
          :db, 
          :logger, 
          :commands_table, 
          :streams_table,
          :offsets_table,
          :consumer_groups_table,
          :events_table
        )
      end
    end
  end
end
