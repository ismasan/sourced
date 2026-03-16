#!/usr/bin/env ruby
# frozen_string_literal: true

# Benchmark: measures claim_next performance across orders of magnitude.
# Seeds data directly via SQL for large scales, then measures a sample.
#
# Usage:
#   bundle exec ruby bench/scaling_bench.rb
#   bundle exec ruby bench/scaling_bench.rb --scales 10,100,1000
#   bundle exec ruby bench/scaling_bench.rb --keys 1,2
#   bundle exec ruby bench/scaling_bench.rb --output results.html

require 'bundler/setup'
require 'sequel'
require 'sourced'
require 'sourced/ccc'
require 'sourced/ccc/store'
require 'optparse'
require 'json'

Sourced.config.logger = Logger.new(File::NULL)
Console.logger.off!

# --- CLI options -----------------------------------------------------------

options = {
  scales: [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000],
  keys: [1, 2, 3],
  iterations: 2,
  sample_size: 50,
  output: 'bench/scaling_results.html'
}

OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options]"
  opts.on('--scales LIST', 'Comma-separated partition counts') { |v| options[:scales] = v.split(',').map(&:to_i) }
  opts.on('--keys LIST', 'Comma-separated key counts') { |v| options[:keys] = v.split(',').map(&:to_i) }
  opts.on('--iterations N', Integer, 'Iterations per measurement') { |v| options[:iterations] = v }
  opts.on('--sample N', Integer, 'Claims to measure per scenario') { |v| options[:sample_size] = v }
  opts.on('--output FILE', 'HTML output file') { |v| options[:output] = v }
end.parse!

GROUP_ID = 'bench-group'
HANDLED_TYPES = ['scaling_bench.event']
INCR_MAX_SCALE = 10_000  # skip incremental discovery above this

BenchEvent = Sourced::CCC::Message.define('scaling_bench.event') do
  attribute :k0, String
  attribute :k1, String
  attribute :k2, String
  attribute :k3, String
end

# --- Fast SQL seeding ------------------------------------------------------

def seed(count, key_count, caught_up: false)
  db = Sequel.sqlite
  db.run('PRAGMA cache_size = -64000')
  db.run('PRAGMA synchronous = OFF')
  db.run('PRAGMA journal_mode = MEMORY')
  store = Sourced::CCC::Store.new(db)
  store.install!
  store.register_consumer_group(GROUP_ID)

  cg_id = db[:sourced_consumer_groups].where(group_id: GROUP_ID).get(:id)
  now = Time.now.iso8601
  batch = [10_000, count].min

  (0...count).each_slice(batch) do |slice|
    db.transaction do
      # Messages
      db[:sourced_messages].multi_insert(
        slice.map { |i|
          payload = (0...4).map { |k| "\"k#{k}\":\"v#{i}\"" }.join(',')
          { message_id: "m-#{i}", message_type: 'scaling_bench.event', payload: "{#{payload}}", created_at: now }
        }
      )

      # Key pairs
      slice.each { |i|
        (0...key_count).each { |k| db.run("INSERT OR IGNORE INTO sourced_key_pairs (name, value) VALUES ('k#{k}', 'v#{i}')") }
      }

      # Message key pairs
      slice.each { |i|
        (0...key_count).each { |k|
          db.run("INSERT INTO sourced_message_key_pairs (message_position, key_pair_id) SELECT #{i + 1}, id FROM sourced_key_pairs WHERE name = 'k#{k}' AND value = 'v#{i}'")
        }
      }

      if caught_up
        # Offsets
        db[:sourced_offsets].multi_insert(
          slice.map { |i|
            pk = (0...key_count).map { |k| "k#{k}:v#{i}" }.join('|')
            { consumer_group_id: cg_id, partition_key: pk, last_position: i + 1, claimed: 0 }
          }
        )

        # Offset key pairs — bulk via INSERT...SELECT
        (0...key_count).each { |k|
          db.run(<<~SQL)
            INSERT OR IGNORE INTO sourced_offset_key_pairs (offset_id, key_pair_id)
            SELECT o.id, kp.id
            FROM sourced_offsets o
            JOIN sourced_key_pairs kp ON kp.name = 'k#{k}'
              AND kp.value = SUBSTR(o.partition_key, #{k > 0 ? "INSTR(o.partition_key, 'k#{k}:') + #{k.to_s.length + 2}" : '4'}, LENGTH(o.partition_key))
            WHERE o.consumer_group_id = #{cg_id}
              AND o.id NOT IN (SELECT offset_id FROM sourced_offset_key_pairs)
          SQL
        }
      end
    end
    $stderr.print '.'
  end

  if caught_up
    db[:sourced_consumer_groups].where(id: cg_id).update(
      highest_position: count, discovery_position: count, updated_at: now
    )
  end

  # Restore WAL mode for benchmarking
  db.run('PRAGMA synchronous = FULL')
  db.run('PRAGMA journal_mode = WAL')

  [db, store]
end

# Simpler caught_up seeding: parse partition_key to match key_pairs
# For the bulk offset_key_pairs insert, extract the value from partition_key
# which has format "k0:v123" or "k0:v123|k1:v123"
def seed_caught_up_fast(count, key_count, eager: false)
  db = Sequel.sqlite
  db.run('PRAGMA cache_size = -64000')
  db.run('PRAGMA synchronous = OFF')
  db.run('PRAGMA journal_mode = MEMORY')
  store = Sourced::CCC::Store.new(db)
  store.install!
  if eager
    store.register_consumer_group(GROUP_ID, partition_by: partition_keys(key_count))
  else
    store.register_consumer_group(GROUP_ID)
  end

  cg_id = db[:sourced_consumer_groups].where(group_id: GROUP_ID).get(:id)
  now = Time.now.iso8601
  batch = [10_000, count].min

  (0...count).each_slice(batch) do |slice|
    db.transaction do
      # Messages
      db[:sourced_messages].multi_insert(
        slice.map { |i|
          payload = (0...4).map { |k| "\"k#{k}\":\"v#{i}\"" }.join(',')
          { message_id: "m-#{i}", message_type: 'scaling_bench.event', payload: "{#{payload}}", created_at: now }
        }
      )

      # Key pairs + message_key_pairs
      slice.each { |i|
        (0...key_count).each { |k|
          db.run("INSERT OR IGNORE INTO sourced_key_pairs (name, value) VALUES ('k#{k}', 'v#{i}')")
          db.run("INSERT INTO sourced_message_key_pairs (message_position, key_pair_id) SELECT #{i + 1}, id FROM sourced_key_pairs WHERE name = 'k#{k}' AND value = 'v#{i}'")
        }
      }

      # Offsets
      db[:sourced_offsets].multi_insert(
        slice.map { |i|
          pk = (0...key_count).map { |k| "k#{k}:v#{i}" }.join('|')
          { consumer_group_id: cg_id, partition_key: pk, last_position: i + 1, claimed: 0 }
        }
      )

      # Offset key pairs — per-row but using subquery
      slice.each { |i|
        pk = (0...key_count).map { |k| "k#{k}:v#{i}" }.join('|')
        (0...key_count).each { |k|
          db.run("INSERT OR IGNORE INTO sourced_offset_key_pairs (offset_id, key_pair_id) SELECT o.id, kp.id FROM sourced_offsets o, sourced_key_pairs kp WHERE o.partition_key = '#{pk}' AND o.consumer_group_id = #{cg_id} AND kp.name = 'k#{k}' AND kp.value = 'v#{i}'")
        }
      }
    end
    $stderr.print '.'
  end

  db[:sourced_consumer_groups].where(id: cg_id).update(
    highest_position: count, discovery_position: count, updated_at: now
  )
  db.run('PRAGMA synchronous = FULL')
  db.run('PRAGMA journal_mode = WAL')

  [db, store]
end

# --- Measurement helpers ---------------------------------------------------

def measure
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  result = yield
  [Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0, result]
end

def median(values)
  sorted = values.sort
  mid = sorted.size / 2
  sorted.size.odd? ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2.0
end

def partition_keys(n) = (0...n).map { |i| "k#{i}" }

def claim_once(store, key_count)
  store.claim_next(GROUP_ID, partition_by: partition_keys(key_count), handled_types: HANDLED_TYPES, worker_id: 'w-1')
end

# Adaptive iterations — fewer for large scales to keep total time reasonable
def effective_iterations(scale, base)
  case scale
  when 0..10_000 then base
  when 10_001..100_000 then [base, 2].min
  else 1
  end
end

# --- Benchmark runner ------------------------------------------------------

results = []

options[:keys].each do |key_count|
  options[:scales].each do |scale|
    label = "keys=#{key_count} scale=#{format('%10d', scale)}"
    $stderr.print "\n#{label}"
    iters = effective_iterations(scale, options[:iterations])

    row = { keys: key_count, scale: scale }

    # --- 1. Idle poll (all caught up) ---
    $stderr.print " [idle"
    idle_times = []
    iters.times do
      _db, store = seed_caught_up_fast(scale, key_count)
      polls = 5.times.map { measure { claim_once(store, key_count) }.first }
      idle_times << median(polls)
    end
    row[:idle_poll_ms] = (median(idle_times) * 1000).round(4)
    $stderr.print "=#{row[:idle_poll_ms]}ms]"

    # --- 2. Cold drain (sample first N claims) ---
    $stderr.print " [cold"
    sample = [options[:sample_size], scale].min
    drain_times = []
    iters.times do
      _db, store = seed(scale, key_count, caught_up: false)
      times = []
      sample.times do
        t, r = measure { claim_once(store, key_count) }
        break unless r
        store.ack(GROUP_ID, offset_id: r.offset_id, position: r.messages.last.position)
        times << t
      end
      drain_times << median(times) if times.any?
    end
    row[:per_claim_cold_ms] = drain_times.any? ? (median(drain_times) * 1000).round(4) : 0
    $stderr.print "=#{row[:per_claim_cold_ms]}ms]"

    # --- 3. Warm claim (sample N claims with new messages) ---
    $stderr.print " [warm"
    warm_times = []
    iters.times do
      _db, store = seed_caught_up_fast(scale, key_count)
      msgs = (0...sample).map { |i| BenchEvent.new(payload: { k0: "v#{i}", k1: "v#{i}", k2: "v#{i}", k3: "v#{i}" }) }
      store.append(msgs)

      times = []
      sample.times do
        t, r = measure { claim_once(store, key_count) }
        break unless r
        store.ack(GROUP_ID, offset_id: r.offset_id, position: r.messages.last.position)
        times << t
      end
      warm_times << median(times) if times.any?
    end
    row[:per_claim_warm_ms] = warm_times.any? ? (median(warm_times) * 1000).round(4) : 0
    $stderr.print "=#{row[:per_claim_warm_ms]}ms]"

    # --- 4. Incremental discovery (1 new partition) ---
    if scale <= INCR_MAX_SCALE
      $stderr.print " [incr"
      incr_times = []
      iters.times do
        _db, store = seed_caught_up_fast(scale, key_count)
        store.append(BenchEvent.new(payload: { k0: 'vnew', k1: 'vnew', k2: 'vnew', k3: 'vnew' }))
        t, _ = measure { claim_once(store, key_count) }
        incr_times << t
      end
      row[:incremental_ms] = (median(incr_times) * 1000).round(4)
      $stderr.print "=#{row[:incremental_ms]}ms]"
    else
      row[:incremental_ms] = nil
    end

    # --- 5. Eager warm claim (offsets created by append, no discovery) ---
    $stderr.print " [eager-warm"
    eager_warm_times = []
    iters.times do
      _db, store = seed_caught_up_fast(scale, key_count, eager: true)
      msgs = (0...sample).map { |i| BenchEvent.new(payload: { k0: "v#{i}", k1: "v#{i}", k2: "v#{i}", k3: "v#{i}" }) }
      store.append(msgs)

      times = []
      sample.times do
        t, r = measure { claim_once(store, key_count) }
        break unless r
        store.ack(GROUP_ID, offset_id: r.offset_id, position: r.messages.last.position)
        times << t
      end
      eager_warm_times << median(times) if times.any?
    end
    row[:eager_warm_ms] = eager_warm_times.any? ? (median(eager_warm_times) * 1000).round(4) : 0
    $stderr.print "=#{row[:eager_warm_ms]}ms]"

    # --- 6. Eager incremental (1 new partition, offset created by append) ---
    if scale <= INCR_MAX_SCALE
      $stderr.print " [eager-incr"
      eager_incr_times = []
      iters.times do
        _db, store = seed_caught_up_fast(scale, key_count, eager: true)
        store.append(BenchEvent.new(payload: { k0: 'vnew', k1: 'vnew', k2: 'vnew', k3: 'vnew' }))
        t, _ = measure { claim_once(store, key_count) }
        eager_incr_times << t
      end
      row[:eager_incremental_ms] = (median(eager_incr_times) * 1000).round(4)
      $stderr.print "=#{row[:eager_incremental_ms]}ms]"
    else
      row[:eager_incremental_ms] = nil
    end

    results << row
  end
end

# --- CSV output ------------------------------------------------------------

$stderr.puts "\n"
puts "keys,scale,idle_poll_ms,per_claim_cold_ms,per_claim_warm_ms,incremental_ms,eager_warm_ms,eager_incremental_ms"
results.each do |r|
  incr = r[:incremental_ms] ? r[:incremental_ms].to_s : ''
  eager_incr = r[:eager_incremental_ms] ? r[:eager_incremental_ms].to_s : ''
  puts "#{r[:keys]},#{r[:scale]},#{r[:idle_poll_ms]},#{r[:per_claim_cold_ms]},#{r[:per_claim_warm_ms]},#{incr},#{r[:eager_warm_ms]},#{eager_incr}"
end

# --- HTML chart output -----------------------------------------------------

chart_data = {}
options[:keys].each { |k| chart_data[k] = results.select { |r| r[:keys] == k } }

def fmt_scale(s)
  return "#{s / 1_000_000}M" if s >= 1_000_000
  return "#{s / 1_000}K" if s >= 1_000
  s.to_s
end

html = <<~HTML
<!DOCTYPE html>
<html>
<head>
  <title>CCC::Store#claim_next — Scaling Benchmark</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #fafafa; }
    h1 { font-size: 1.3em; color: #333; }
    h2 { font-size: 1.1em; color: #555; margin-top: 2em; }
    .chart-container { background: white; border-radius: 8px; padding: 20px; margin: 16px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .chart-container p.desc { color: #666; font-size: 0.85em; margin: 0 0 12px 0; line-height: 1.5; }
    .chart-container p.desc strong { color: #444; }
    canvas { max-height: 400px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    @media (max-width: 800px) { .grid { grid-template-columns: 1fr; } }
    .meta { color: #888; font-size: 0.85em; margin-bottom: 20px; }
    table { border-collapse: collapse; width: 100%; margin: 16px 0; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #eee; font-variant-numeric: tabular-nums; }
    th { background: #f5f5f5; font-weight: 600; color: #555; }
    td:first-child, th:first-child { text-align: left; }
  </style>
</head>
<body>
  <h1>CCC::Store#claim_next &mdash; Scaling Benchmark</h1>
  <p class="meta">
    Generated #{Time.now.strftime('%Y-%m-%d %H:%M')} &middot;
    Keys: #{options[:keys].join(', ')} &middot;
    Scales: #{options[:scales].map { |s| fmt_scale(s) }.join(', ')} &middot;
    Iterations: #{options[:iterations]} (adaptive) &middot;
    Sample: #{options[:sample_size]} claims
  </p>

  <div class="grid">
    <div class="chart-container">
      <p class="desc"><strong>Idle Poll</strong> &mdash; All partitions are fully caught up. No new messages exist.
      This is what happens on every catch-up poll interval when the system is quiet.
      Measures the cost of determining &ldquo;nothing to do&rdquo;.</p>
      <canvas id="idlePoll"></canvas>
    </div>
    <div class="chart-container">
      <p class="desc"><strong>Cold Drain</strong> &mdash; A new consumer group starts processing an existing log from scratch.
      No offsets exist yet &mdash; each <code>claim_next</code> call must discover new partitions, create offsets,
      and claim work. This is the per-claim cost during initial catch-up (sampled over #{options[:sample_size]} claims).</p>
      <canvas id="coldDrain"></canvas>
    </div>
    <div class="chart-container">
      <p class="desc"><strong>Warm Claim</strong> &mdash; All partitions have existing offsets (previously caught up),
      then new messages arrive for some partitions. Legacy path runs discovery to advance watermark.
      This is the steady-state cost when the notifier or catch-up poller triggers processing.</p>
      <canvas id="warmClaim"></canvas>
    </div>
    <div class="chart-container">
      <p class="desc"><strong>Eager Warm Claim</strong> &mdash; Same as Warm Claim, but with <code>partition_by</code>
      registered. Offsets are created during <code>append</code>, so <code>claim_next</code> goes straight
      to the fast path &mdash; no discovery CTE needed.</p>
      <canvas id="eagerWarm"></canvas>
    </div>
    <div class="chart-container">
      <p class="desc"><strong>Incremental Discovery</strong> &mdash; All existing partitions are caught up.
      One message arrives for a brand-new partition (never seen before). Measures the cost of discovering
      and claiming that single new partition against a backdrop of N existing offsets.
      Skipped for scales &gt; #{fmt_scale(INCR_MAX_SCALE)} due to prohibitive NOT EXISTS cost.</p>
      <canvas id="incremental"></canvas>
    </div>
    <div class="chart-container">
      <p class="desc"><strong>Eager Incremental</strong> &mdash; Same scenario as Incremental, but with eager offset
      creation. The offset is created during <code>append</code>, so <code>claim_next</code> finds it
      on the fast path without running the discovery CTE.
      Skipped for scales &gt; #{fmt_scale(INCR_MAX_SCALE)}.</p>
      <canvas id="eagerIncremental"></canvas>
    </div>
  </div>

  <h2>Raw Data</h2>
  <table>
    <thead>
      <tr><th>Keys</th><th>Partitions</th><th>Idle Poll (ms)</th><th>Cold /claim (ms)</th><th>Warm (ms)</th><th>Eager Warm (ms)</th><th>Incr (ms)</th><th>Eager Incr (ms)</th></tr>
    </thead>
    <tbody>
      #{results.map { |r|
        scale_fmt = r[:scale].to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse
        incr_fmt = r[:incremental_ms] ? r[:incremental_ms].to_s : '—'
        eager_incr_fmt = r[:eager_incremental_ms] ? r[:eager_incremental_ms].to_s : '—'
        "<tr><td>#{r[:keys]}</td><td>#{scale_fmt}</td><td>#{r[:idle_poll_ms]}</td><td>#{r[:per_claim_cold_ms]}</td><td>#{r[:per_claim_warm_ms]}</td><td>#{r[:eager_warm_ms]}</td><td>#{incr_fmt}</td><td>#{eager_incr_fmt}</td></tr>"
      }.join("\n      ")}
    </tbody>
  </table>

  <script>
    const data = #{JSON.pretty_generate(chart_data)};
    const colors = { 1: '#2196F3', 2: '#FF9800', 3: '#4CAF50', 4: '#9C27B0' };

    function makeChart(id, title, field, yLabel) {
      const datasets = Object.entries(data).map(([keys, rows]) => ({
        label: keys + (keys === '1' ? ' key' : ' keys'),
        data: rows.filter(r => r[field] != null).map(r => ({ x: r.scale, y: r[field] })),
        borderColor: colors[keys],
        backgroundColor: colors[keys] + '20',
        tension: 0.3,
        pointRadius: 4,
        borderWidth: 2
      }));

      new Chart(document.getElementById(id), {
        type: 'line',
        data: { datasets },
        options: {
          responsive: true,
          plugins: { title: { display: true, text: title, font: { size: 14 } } },
          scales: {
            x: {
              type: 'logarithmic',
              title: { display: true, text: 'Partition count' },
              ticks: {
                callback: function(val) {
                  if (val >= 1000000) return (val/1000000) + 'M';
                  if (val >= 1000) return (val/1000) + 'K';
                  return val;
                }
              }
            },
            y: {
              type: 'logarithmic',
              title: { display: true, text: yLabel },
              ticks: {
                callback: function(val) {
                  if (val >= 1000) return (val/1000).toFixed(0) + 's';
                  return val.toFixed(1);
                }
              }
            }
          }
        }
      });
    }

    makeChart('idlePoll', 'Idle Poll (all caught up, no work)', 'idle_poll_ms', 'ms');
    makeChart('coldDrain', 'Cold Drain (per-claim cost, sampled)', 'per_claim_cold_ms', 'ms / claim');
    makeChart('warmClaim', 'Warm Claim — legacy (per-claim cost)', 'per_claim_warm_ms', 'ms / claim');
    makeChart('eagerWarm', 'Eager Warm Claim (per-claim cost)', 'eager_warm_ms', 'ms / claim');
    makeChart('incremental', 'Incremental Discovery — legacy (1 new partition)', 'incremental_ms', 'ms');
    makeChart('eagerIncremental', 'Eager Incremental (1 new partition)', 'eager_incremental_ms', 'ms');
  </script>
</body>
</html>
HTML

File.write(options[:output], html)
$stderr.puts "Chart written to #{options[:output]}"
