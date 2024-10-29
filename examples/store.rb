# frozen_string_literal: true

require 'bundler'
Bundler.setup(:test)

require 'sors'
require 'sequel'

Sequel.extension :fiber_concurrency
DB = Sequel.postgres('event_store')
# DB.logger = Logger.new($stdout)

class Store
  CONSUMER_STATS_SQL = <<~SQL
    SELECT 
        group_id,
        min(global_seq) as oldest_processed,
        max(global_seq) as newest_processed,
        count(*) as stream_count
    FROM offsets
    GROUP BY group_id;
  SQL

  attr :db

  def initialize(db)
    @db = db
  end

  ConsumerStats = Data.define(:stream_count, :max_global_seq, :groups)

  def consumer_stats
    stream_count = db[:streams].count
    max_global_seq = db[:events].max(:global_seq)
    groups = db.fetch(CONSUMER_STATS_SQL).all
    ConsumerStats.new(stream_count, max_global_seq, groups)
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
      row = db.fetch(RESERVE_SQL, group_id, group_id, group_id).first
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
class Worker
  attr_reader :name

  def initialize(name, store)
    @name = name
    @store = store
    @running = false
  end

  def stop
    @running = false
  end

  def poll(&)
    @running = true
    while @running
      @store.reserve_next_for(name, &)
      sleep 0.01
    end
  end
end

class Reactor
  class << self
    def registry
      @registry ||= {}
    end

    def reactor_name = name

    def register(klass)
      registry[klass.reactor_name] = klass
    end

    def handle_event(event)
      puts "Worker #{self.reactor_name} processing #{event.stream_id} #{event.seq}"
    end
  end
end

class SalesReport < Reactor
end

class Listings < Reactor
end

Reactor.register(SalesReport)
Reactor.register(Listings)

WORKERS = 10.times.map { |i| Worker.new("worker-#{i}", STORE) }

trap('INT') { WORKERS.each(&:stop) }

Sync do |task|
  WORKERS.each do |w|
    task.async do
      w.poll do |event|
        puts "Worker #{w.name} processing #{event.stream_id} #{event.seq}"
      end
    end
  end
end

puts 'bye'
