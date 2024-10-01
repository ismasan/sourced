# frozen_string_literal: true

require 'sequel'
require 'async'
require 'json'
require 'console' #  comes with Async
require 'sors/message'

Sequel.extension :fiber_concurrency
Sequel.extension :pg_json if defined?(PG)

module Sors
  module Backends
    class SequelBackend
      def initialize(db, prefix: 'sors')
        @db = connect(db)
        @prefix = prefix
        @events_table = table_name(:events)
        @streams_table = table_name(:streams)
        @commands_table = table_name(:commands)
        Console.info("Connected to #{@db}")
      end

      def installed?
        db.table_exists?(events_table) && db.table_exists?(streams_table) && db.table_exists?(commands_table)
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

      def reserve_next(&)
        command = db.transaction do
          cmd = db[commands_table]
            .join(streams_table, stream_id: :stream_id)
            .where(Sequel[streams_table][:locked] => false)
            .order(Sequel[commands_table][:id])
            .for_update
            .first

          if cmd
            db[streams_table].where(stream_id: cmd[:stream_id]).update(locked: true)
          end
          cmd
        end

        if command
          data = command[:data]
          # Support SQlite
          # TODO: figure out how to handle this in a better way
          data = parse_json(data)
          yield Message.from(data)
          # Only delete the command if processing didn't raise
          db[commands_table].where(id: command[:id]).delete
        end
        command
      ensure
        # Always unlock the stream
        if command
          db[streams_table].where(stream_id: command[:stream_id]).update(locked: false)
        end
      end

      def transaction(&)
        db.transaction(&)
      end

      def append_events(events)
        rows = events.map { |e| serialize_event(e) }
        db[events_table].multi_insert(rows)
        true
      end

      def read_event_batch(causation_id)
        db[events_table].where(causation_id:).order(:global_seq).map do |row|
          deserialize_event(row)
        end
      end

      def read_event_stream(stream_id)
        db[events_table].where(stream_id:).order(:global_seq).map do |row|
          deserialize_event(row)
        end
      end

      def install
        db.create_table?(events_table) do
          # @db is the local @ivar in Sequel::Generator
          if @db.class.name == 'Sequel::SQLite::Database'
            # auto increment integer for sqlite
            Bignum :global_seq, type: 'INTEGER PRIMARY KEY AUTOINCREMENT'
          else
            Bignum :global_seq, primary_key: true, default: Sequel.function(:nextval, 'events_global_seq')
          end
          column :id, :uuid, unique: true
          String :stream_id, null: false, index: true
          String :type, null: false
          Time :created_at, null: false
          String :producer
          column :causation_id, :uuid, index: true
          column :correlation_id, :uuid
          column :payload, :jsonb
        end
        Console.info("Created table #{events_table}")

        db.create_table?(streams_table) do
          String :stream_id, primary_key: true, unique: true
          column :locked, :boolean, default: false, null: false
        end
        Console.info("Created table #{streams_table}")

        # Define in local scope so that it can be used in the block
        _streams_table = streams_table
        db.create_table?(commands_table) do
          primary_key :id
          foreign_key :stream_id, _streams_table, type: String, null: false
          column :data, :jsonb, null: false
          if @db.class.name == 'Sequel::SQLite::Database'
            Time :scheduled_at, null: false, type: 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
          else
            Time :scheduled_at, null: false, default: Sequel.function(:now)
          end
        end
        Console.info("Created table #{commands_table}")
        self
      end

      private

      attr_reader :db, :prefix, :events_table, :streams_table, :commands_table

      def table_name(name)
        [prefix, name].join('_').to_sym
      end

      def parse_json(json)
        return json unless json.is_a?(String)

        JSON.parse(json, symbolize_names: true)
      end

      def serialize_event(event)
        row = event.to_h
        row[:payload] = JSON.dump(row[:payload]) if row[:payload]
        row
      end

      def deserialize_event(row)
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
