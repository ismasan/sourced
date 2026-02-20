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

    class Store
      attr_reader :db

      def initialize(db)
        @db = db
        @db.run('PRAGMA foreign_keys = ON')
        @db.run('PRAGMA journal_mode = WAL')
        @db.run('PRAGMA busy_timeout = 5000')
      end

      def installed?
        db.table_exists?(:ccc_messages) &&
          db.table_exists?(:ccc_key_pairs) &&
          db.table_exists?(:ccc_message_key_pairs)
      end

      def install!
        db.run(<<~SQL)
          CREATE TABLE IF NOT EXISTS ccc_messages (
            position INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            message_type TEXT NOT NULL,
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
      end

      # Append messages to the store. Extracts keys and indexes them.
      # Returns the last assigned position.
      def append(messages)
        messages = Array(messages)
        return latest_position if messages.empty?

        last_position = nil

        db.transaction do
          messages.each do |msg|
            payload_json = msg.payload ? JSON.dump(msg.payload.to_h) : '{}'
            metadata_json = msg.metadata.empty? ? nil : JSON.dump(msg.metadata)

            db[:ccc_messages].insert(
              message_id: msg.id,
              message_type: msg.type,
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

      # Query messages by conditions (array of QueryCondition).
      # Each condition matches (message_type AND key_name/key_value).
      # Conditions are OR'd together.
      def read(conditions, from_position: nil, limit: nil)
        conditions = Array(conditions)
        return [] if conditions.empty?

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
          SELECT DISTINCT m.position, m.message_id, m.message_type, m.payload, m.metadata, m.created_at
          FROM ccc_messages m
          JOIN ccc_message_key_pairs mkp ON m.position = mkp.message_position
          WHERE (#{where_parts.join(' OR ')})
        SQL

        sql += " AND m.position > #{db.literal(from_position)}" if from_position
        sql += ' ORDER BY m.position'
        sql += " LIMIT #{db.literal(limit)}" if limit

        db.fetch(sql).map { |row| deserialize(row) }
      end

      # Conflict detection: returns messages matching conditions that appeared
      # after the given position. Empty array means no conflicts.
      def messages_since(conditions, position)
        read(conditions, from_position: position)
      end

      # Current max position, or 0 if the store is empty.
      def latest_position
        db[:ccc_messages].max(:position) || 0
      end

      # Clear all tables. For testing only.
      def clear!
        db[:ccc_message_key_pairs].delete
        db[:ccc_key_pairs].delete
        db[:ccc_messages].delete
        db.run('DELETE FROM sqlite_sequence') if db.table_exists?(:sqlite_sequence)
      end

      private

      def deserialize(row)
        payload = JSON.parse(row[:payload], symbolize_names: true)
        metadata = row[:metadata] ? JSON.parse(row[:metadata], symbolize_names: true) : {}

        klass = Message.registry[row[:message_type]]
        attrs = {
          id: row[:message_id],
          type: row[:message_type],
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
