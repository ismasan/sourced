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
      case command
      when Machine::ProcessBatch
        puts "BATCH #{worker.name} received command: #{command.inspect}"
        batch = ES.read_batch(command.causation_id)
        reactors = Router.reactors_for(batch)
        # TODO: reactors should work concurrently
        # either using a thread/fiber pool here, or by scheduling
        # execution as individual commands
        # Or by passing reactors to other threads in this group
        # However, if a reactor fails in a thread, this block will be unaware
        # and the original batch command will be lost
        # Perhaps: Machine::RunReactor.new(reactor_name, batch_id)
        commands = reactors.flat_map { |reactor| reactor.call(batch) }
        puts "BATCH #{worker.name} schedulling commands: #{commands.inspect}"
        worker.call(commands)
      else
        puts "Worker #{worker.name} received command: #{command.inspect}"
        Router.handle(command)
      end
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
