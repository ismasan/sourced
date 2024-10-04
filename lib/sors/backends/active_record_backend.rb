# frozen_string_literal: true

require 'active_record'
require 'json'
require 'sors/message'

module Sors
  module Backends
    class ActiveRecordBackend
      PREFIX = 'sors_'

      class EventRecord < ActiveRecord::Base
        self.inheritance_column = nil
        self.table_name = [PREFIX, 'events'].join
      end

      class StreamRecord < ActiveRecord::Base
        self.table_name = [PREFIX, 'streams'].join
      end

      class CommandRecord < ActiveRecord::Base
        self.table_name = [PREFIX, 'commands'].join
      end

      def initialize; end

      def clear!
        raise 'Not in test environment' unless ENV['ENVIRONMENT'] == 'test'

        EventRecord.delete_all
        CommandRecord.delete_all
        StreamRecord.delete_all
      end

      def installed?
        ActiveRecord::Base.connection.table_exists?(EventRecord.table_name) &&
          ActiveRecord::Base.connection.table_exists?(StreamRecord.table_name) &&
          ActiveRecord::Base.connection.table_exists?(CommandRecord.table_name)
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
        event.to_h
      end

      def deserialize_event(record)
        Message.from(record.attributes.deep_symbolize_keys)
      end
    end
  end
end
