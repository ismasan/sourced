require 'thread'
require_relative 'test'

THREADS = 4
WORKERS = THREADS.times.map do |i|
  Commander.new(DB, i)
end

trap('INT') { WORKERS.each(&:stop) }

WORKERS.map do |worker|
  Thread.new do
    worker.poll do |command|
      puts "Worker #{worker.name} received command: #{command.inspect}"
      Router.handle(command)
    end
  end
end.map(&:join)

puts 'Bye bye'
# count = 1
# COMMANDS.poll do |command|
#   puts "Received command: id:#{command[:id]} stream_id:#{command[:stream_id]} name:#{command[:data][:name]}"
#   # sleep 1
#   count += 1
#   COMMANDS.stop if count > 100
# end
