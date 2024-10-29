# frozen_string_literal: true

require 'bundler'
Bundler.setup(:test)

require 'sors'
require 'sequel'

DB = Sequel.postgres('event_store')
# DB.logger = Logger.new($stdout)

class Store
  attr :db

  def initialize(db)
    @db = db
  end

  def append_to_stream(stream_id, events)
    return if events.empty?

    db.transaction do
      seq = events.last.seq
      id = db[:streams].insert_conflict(target: :stream_id, update: { seq:, updated_at: Time.now }).insert(stream_id:, seq:)#.first[:id]
      rows = events.map { |e| serialize_event(e, id) }
      db[:events].multi_insert(rows)
    end
  end

  def stream_seq(stream_id)
    str  = db[:streams].select(:seq).where(stream_id:).first
    str ? str[:seq] : 0
  end

  RESERVE_SQL = <<~SQL
    WITH next_event AS (
        SELECT
            e.global_seq,
            s.id AS stream_id_fk,
            s.stream_id,
            e.id,
            e.seq,
            e.type,
            e.created_at,
            e.producer,
            e.causation_id,
            e.correlation_id,
            e.payload,
            o.id AS offset_id
        FROM
            events e
        JOIN
            streams s ON e.stream_id = s.id
        LEFT JOIN
            offsets o ON o.stream_id = e.stream_id AND o.group_id = ?
        WHERE
            -- Only select events with a higher sequence number than the last processed
            (o.global_seq IS NULL OR e.global_seq > o.global_seq)
        ORDER BY
            e.global_seq
        LIMIT 1
    )
    SELECT *
    FROM next_event
  SQL

  RESERVE2_SQL = <<~SQL
      SELECT 
          e.global_seq,
          e.id,
          s.id as stream_id_fk,
          s.stream_id,
          e.seq,
          e.type,
          e.payload,
          e.created_at
      FROM events e
      JOIN
          streams s ON e.stream_id = s.id
      WHERE e.global_seq > COALESCE(
          (SELECT o.global_seq 
          FROM offsets o 
          WHERE o.group_id = ?
          AND o.stream_id = s.id
          FOR UPDATE SKIP LOCKED),
          0
      )
      ORDER BY e.global_seq
      LIMIT 1
  SQL

  RESERVE3_SQL = <<~SQL
    WITH available_offsets AS (
        SELECT o.stream_id, o.global_seq
        FROM offsets o
        WHERE o.group_id = ?
        FOR UPDATE SKIP LOCKED
    ),
    next_event AS (
        SELECT 
            e.global_seq,
            e.id,
            s.id as stream_id_fk,
            s.stream_id,
            e.seq,
            e.type,
            e.causation_id,
            e.correlation_id,
            e.payload,
            e.created_at
        FROM events e
        JOIN streams s ON e.stream_id = s.id
        LEFT JOIN available_offsets a ON s.id = a.stream_id
        WHERE e.global_seq > COALESCE(a.global_seq, 0)
        ORDER BY e.global_seq
        LIMIT 1
    )
    SELECT 
        ne.global_seq,
        ne.id,
        ne.stream_id,
        ne.stream_id_fk,
        ne.seq,
        ne.type,
        ne.causation_id,
        ne.correlation_id,
        ne.payload,
        ne.created_at
    FROM next_event ne;
  SQL

  RESERVE4_SQL = <<~SQL
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
        FROM events e
        JOIN streams s ON e.stream_id = s.id
        LEFT JOIN offsets o ON o.stream_id = e.stream_id 
            AND o.group_id = ?
        WHERE e.global_seq > COALESCE(o.global_seq, 0)
        ORDER BY e.global_seq
    ),
    next_event AS (
        SELECT *
        FROM candidate_events
        WHERE lock_obtained = true
        LIMIT 1
    ),
    locked_offset AS (
        INSERT INTO offsets (stream_id, group_id, global_seq)
        SELECT 
            ne.stream_id_fk,
            ?,
            0
        FROM next_event ne
        ON CONFLICT (group_id, stream_id) DO UPDATE 
        SET global_seq = offsets.global_seq
        RETURNING id, stream_id, global_seq
    )
    SELECT 
        ne.*,
        lo.id AS offset_id
    FROM next_event ne
    JOIN locked_offset lo ON ne.stream_id_fk = lo.stream_id;
  SQL

  def reserve_next_for(group_id, &)
    db.transaction do
      row = db.fetch(RESERVE4_SQL, group_id, group_id, group_id).first
      return unless row

      event = Sors::Message.from(row)
      if block_given?
        yield event

        db[:offsets].where(id: row[:offset_id]).update(global_seq: row[:global_seq])
      end

      [row[:offset_id], row[:stream_id], row[:global_seq]]
    end
  end

  def serialize_event(event, stream_id)
    row = event.to_h
    row[:stream_id] = stream_id
    row[:payload] = JSON.dump(row[:payload]) if row[:payload]
    row
  end

  def deserialize_event(row)
    row[:payload] = parse_json(row[:payload]) if row[:payload]
    Sors::Message.from(row)
  end

  def parse_json(json)
    return json unless json.is_a?(String)

    JSON.parse(json, symbolize_names: true)
  end
end

TestCommand = Sors::Message.define('test_command') do
  attribute :number, Integer
end

TestEvent = Sors::Message.define('test_event') do
  attribute :number, Integer
end

STORE = Store.new(DB)
streams = %w[stream1 stream2 stream3 stream4 stream5]

# commands = 100.times.map do |i|
#   stream_id = streams.sample
#   TestCommand.parse(stream_id:, payload: { number: i })
# end
#
# 1000.times do |i|
#   cmd = commands.sample
#   seq = STORE.stream_seq(cmd.stream_id)
#   events = rand(1..5).times.map do
#     seq += 1
#     cmd.follow_with_seq(TestEvent, seq, number: i)
#   end
#   STORE.append_to_stream(cmd.stream_id, events)
# end
