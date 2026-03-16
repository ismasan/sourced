#!/usr/bin/env ruby
# frozen_string_literal: true

# Benchmark: measures Store#append performance across message counts
# and payload sizes. Baseline before eager offset creation.
#
# Usage:
#   bundle exec ruby bench/append_bench.rb
#   bundle exec ruby bench/append_bench.rb --counts 1,10,100
#   bundle exec ruby bench/append_bench.rb --keys 1,3
#   bundle exec ruby bench/append_bench.rb --output bench/append_results.html

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
  counts: [1, 10, 50, 100],       # messages per append call
  keys: [1, 2, 3],                 # payload attributes used as partition keys
  iterations: 5,                   # iterations per measurement
  pre_existing: [0, 1_000, 10_000], # pre-existing messages in the store
  output: 'bench/append_results.html'
}

OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options]"
  opts.on('--counts LIST', 'Messages per append call') { |v| options[:counts] = v.split(',').map(&:to_i) }
  opts.on('--keys LIST', 'Key counts') { |v| options[:keys] = v.split(',').map(&:to_i) }
  opts.on('--iterations N', Integer, 'Iterations') { |v| options[:iterations] = v }
  opts.on('--pre LIST', 'Pre-existing message counts') { |v| options[:pre_existing] = v.split(',').map(&:to_i) }
  opts.on('--output FILE', 'HTML output') { |v| options[:output] = v }
end.parse!

# --- Message definitions --------------------------------------------------

AppendBenchEvent = Sourced::CCC::Message.define('append_bench.event') do
  attribute :k0, String
  attribute :k1, String
  attribute :k2, String
end

# --- Helpers ---------------------------------------------------------------

def new_store
  db = Sequel.sqlite
  store = Sourced::CCC::Store.new(db)
  store.install!
  [db, store]
end

def seed_messages(db, count)
  return if count == 0
  now = Time.now.iso8601
  (0...count).each_slice(10_000) do |slice|
    db.transaction do
      db[:sourced_messages].multi_insert(
        slice.map { |i|
          { message_id: "seed-#{i}", message_type: 'append_bench.seed', payload: '{"x":"y"}', created_at: now }
        }
      )
    end
  end
end

def build_messages(count, key_count, offset: 0)
  count.times.map do |i|
    n = offset + i
    payload = {}
    payload[:k0] = "v#{n}" if key_count >= 1
    payload[:k1] = "v#{n}" if key_count >= 2
    payload[:k2] = "v#{n}" if key_count >= 3
    AppendBenchEvent.new(payload: payload)
  end
end

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

def fmt_scale(s)
  return "#{s / 1_000_000}M" if s >= 1_000_000
  return "#{s / 1_000}K" if s >= 1_000
  s.to_s
end

# --- Benchmark runner ------------------------------------------------------

results = []

options[:keys].each do |key_count|
  options[:pre_existing].each do |pre|
    options[:counts].each do |count|
      label = "keys=#{key_count} pre=#{fmt_scale(pre)} count=#{count}"
      $stderr.print "  #{label}..."

      times = []
      per_msg_times = []

      options[:iterations].times do
        _db, store = new_store
        seed_messages(_db, pre)

        msgs = build_messages(count, key_count)

        elapsed, _ = measure { store.append(msgs) }
        times << elapsed
        per_msg_times << elapsed / count
      end

      row = {
        keys: key_count,
        pre_existing: pre,
        count: count,
        total_ms: (median(times) * 1000).round(3),
        per_msg_ms: (median(per_msg_times) * 1000).round(3)
      }
      results << row
      $stderr.puts " #{row[:total_ms]}ms total, #{row[:per_msg_ms]}ms/msg"
    end
  end
end

# --- CSV output ------------------------------------------------------------

puts "keys,pre_existing,count,total_ms,per_msg_ms"
results.each do |r|
  puts "#{r[:keys]},#{r[:pre_existing]},#{r[:count]},#{r[:total_ms]},#{r[:per_msg_ms]}"
end

# --- HTML chart output -----------------------------------------------------

# Group by (keys, pre_existing) for charting
chart_data = {}
options[:keys].each do |k|
  options[:pre_existing].each do |pre|
    label = "#{k} key#{k > 1 ? 's' : ''}, #{fmt_scale(pre)} existing"
    chart_data[label] = results.select { |r| r[:keys] == k && r[:pre_existing] == pre }
  end
end

colors = [
  '#2196F3', '#FF9800', '#4CAF50', '#9C27B0', '#F44336',
  '#00BCD4', '#FF5722', '#8BC34A', '#3F51B5', '#CDDC39'
]

html = <<~HTML
<!DOCTYPE html>
<html>
<head>
  <title>CCC::Store#append — Benchmark</title>
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
  <h1>CCC::Store#append &mdash; Benchmark</h1>
  <p class="meta">
    Generated #{Time.now.strftime('%Y-%m-%d %H:%M')} &middot;
    Keys: #{options[:keys].join(', ')} &middot;
    Batch sizes: #{options[:counts].join(', ')} &middot;
    Pre-existing: #{options[:pre_existing].map { |s| fmt_scale(s) }.join(', ')} &middot;
    Iterations: #{options[:iterations]}
  </p>

  <div class="grid">
    <div class="chart-container">
      <p class="desc"><strong>Total append time</strong> &mdash; Wall-clock time for a single
      <code>store.append(messages)</code> call. Includes message insertion, key_pair
      extraction/dedup, and message_key_pairs indexing. Varies with batch size
      and number of payload attributes (keys).</p>
      <canvas id="totalTime"></canvas>
    </div>
    <div class="chart-container">
      <p class="desc"><strong>Per-message cost</strong> &mdash; Total time divided by message count.
      Shows the marginal cost of each additional message in the batch. Should be
      roughly constant if append scales linearly with batch size.</p>
      <canvas id="perMsg"></canvas>
    </div>
  </div>

  <h2>Raw Data</h2>
  <table>
    <thead>
      <tr><th>Keys</th><th>Pre-existing</th><th>Batch size</th><th>Total (ms)</th><th>Per-msg (ms)</th></tr>
    </thead>
    <tbody>
      #{results.map { |r|
        pre_fmt = r[:pre_existing].to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse
        "<tr><td>#{r[:keys]}</td><td>#{pre_fmt}</td><td>#{r[:count]}</td><td>#{r[:total_ms]}</td><td>#{r[:per_msg_ms]}</td></tr>"
      }.join("\n      ")}
    </tbody>
  </table>

  <script>
    const data = #{JSON.pretty_generate(chart_data)};
    const colorList = #{JSON.generate(colors)};

    function makeChart(id, title, field, yLabel) {
      let i = 0;
      const datasets = Object.entries(data).map(([label, rows]) => {
        const color = colorList[i++ % colorList.length];
        return {
          label: label,
          data: rows.map(r => ({ x: r.count, y: r[field] })),
          borderColor: color,
          backgroundColor: color + '20',
          tension: 0.3,
          pointRadius: 4,
          borderWidth: 2
        };
      });

      new Chart(document.getElementById(id), {
        type: 'line',
        data: { datasets },
        options: {
          responsive: true,
          plugins: { title: { display: true, text: title, font: { size: 14 } } },
          scales: {
            x: { type: 'linear', title: { display: true, text: 'Messages per append' }, min: 0 },
            y: { title: { display: true, text: yLabel }, beginAtZero: true }
          }
        }
      });
    }

    makeChart('totalTime', 'Total Append Time', 'total_ms', 'ms');
    makeChart('perMsg', 'Per-Message Cost', 'per_msg_ms', 'ms / message');
  </script>
</body>
</html>
HTML

File.write(options[:output], html)
$stderr.puts "\nChart written to #{options[:output]}"
