# frozen_string_literal: true

require 'active_record'
require 'json'
require 'sors/message'

module Sors
  module Backends
    class ActiveRecordBackend
      PREFIX = 'sors'

      class EventRecord < ActiveRecord::Base
        self.inheritance_column = nil
        self.table_name = [PREFIX, '_events'].join
      end

      class StreamRecord < ActiveRecord::Base
        self.table_name = [PREFIX, '_streams'].join
      end

      class CommandRecord < ActiveRecord::Base
        self.table_name = [PREFIX, '_commands'].join
      end

      def self.table_prefix=(prefix)
        @table_prefix = prefix
        EventRecord.table_name = [prefix, '_events'].join
        StreamRecord.table_name = [prefix, '_streams'].join
        CommandRecord.table_name = [prefix, '_commands'].join
      end

      def self.table_prefix
        @table_prefix || PREFIX
      end

      def self.installed?
        ActiveRecord::Base.connection.table_exists?(EventRecord.table_name) &&
          ActiveRecord::Base.connection.table_exists?(StreamRecord.table_name) &&
          ActiveRecord::Base.connection.table_exists?(CommandRecord.table_name)
      end

      def self.uninstall!
        raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

        ActiveRecord::Base.connection.drop_table(EventRecord.table_name)
        ActiveRecord::Base.connection.drop_table(CommandRecord.table_name)
        ActiveRecord::Base.connection.drop_table(StreamRecord.table_name)
      end

      def initialize
        @serialize_event = method(:serialize_jsonb_event)
        @deserialize_event = method(:deserialize_jsonb_event)
        if EventRecord.connection.class.name == 'ActiveRecord::ConnectionAdapters::SQLite3Adapter'
          @serialize_event = method(:serialize_sqlite_event)
          @deserialize_event = method(:deserialize_sqlite_event)
        end
      end

      def installed? = self.class.installed?

      def clear!
        raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

        EventRecord.delete_all
        CommandRecord.delete_all
        StreamRecord.delete_all
      end

      # TODO: if all commands belong to the same stream_id
      # we could upsert the streams table once here
      def schedule_commands(commands)
        return false if commands.empty?

        # TODO: here we could use multi_insert
        # for both streams and commands
        CommandRecord.transaction do
          commands.each do |command|
            schedule_command(command.stream_id, command)
          end
        end
        true
      end

      def schedule_command(stream_id, command)
        CommandRecord.transaction do
          StreamRecord.upsert({ stream_id: }, unique_by: :stream_id)
          CommandRecord.create!(stream_id:, data: command.to_json)
        end
      end

      # TODO: locking the stream could be 
      # done in a single SQL query, or using an SQL function.
      def reserve_next(&)
        command_record = transaction do
          cmd = CommandRecord
            .joins("INNER JOIN #{StreamRecord.table_name} ON #{CommandRecord.table_name}.stream_id = #{StreamRecord.table_name}.stream_id")
            .where(["#{StreamRecord.table_name}.locked = ?", false])
            .order("#{CommandRecord.table_name}.id ASC")
            .lock # "FOR UPDATE"
            .first

          if cmd
            StreamRecord.where(stream_id: cmd.stream_id).update(locked: true)
          end
          cmd
        end

        cmd = nil
        if command_record
          # TODO: find out why #data isn't already
          # deserialized here
          data = JSON.parse(command_record.data, symbolize_names: true)
          cmd = Message.from(data)
          yield cmd
          # Only delete the command if processing didn't raise
          command_record.destroy
        end
        cmd
      ensure
        # Always unlock the stream
        if command_record
          StreamRecord.where(stream_id: command_record.stream_id).update(locked: false)
        end
      end

      def transaction(&)
        EventRecord.transaction(&)
      end

      def append_events(events)
        rows = events.map { |e| serialize_event(e) }
        EventRecord.insert_all(rows)
        true
      end

      def read_event_batch(causation_id)
        EventRecord.where(causation_id:).order(:global_seq).map do |record|
          deserialize_event(record)
        end
      end

      def read_event_stream(stream_id)
        EventRecord.where(stream_id:).order(:global_seq).map do |record|
          deserialize_event(record)
        end
      end

      private

      def serialize_event(event)
        @serialize_event.(event)
      end

      def serialize_jsonb_event(event)
        event.to_h
      end

      def serialize_sqlite_event(event)
        attrs = event.to_h
        attrs[:payload] = JSON.generate(attrs[:payload]) if attrs[:payload].present?
        attrs
      end

      def deserialize_event(record)
        Message.from(@deserialize_event.(record).deep_symbolize_keys)
      end

      def deserialize_jsonb_event(record)
        record.attributes
      end

      def deserialize_sqlite_event(record)
        attrs = record.attributes
        attrs['payload'] = JSON.parse(attrs['payload']) if attrs['payload'].present?
        attrs
      end
    end
  end
end
