require_relative 'test'

THREADS = 4
WORKERS = THREADS.times.map do |i|
  Commander.new(DB, i)
end

trap('INT') { WORKERS.each(&:stop) }

Sync do
  WORKERS.map do |worker|
    Async do
      worker.poll do |command|
        case command
        when Machine::ProcessBatch
          Console.warn "BATCH #{worker.name} received command: #{command.inspect}"
          batch = ES.read_batch(command.causation_id)
          reactors = Router.reactors_for(batch)
          # TODO: reactors should work concurrently
          # either using a thread/fiber pool here, or by scheduling
          # execution as individual commands
          # Or by passing reactors to other threads in this group
          # However, if a reactor fails in a thread, this block will be unaware
          # and the original batch command will be lost
          # Perhaps: Machine::RunReactor.new(reactor_name, batch_id)

          # Run this batch of events through any reactors
          # that are interested in them
          # Each reactor runs in a separate Fiber
          runs = reactors.map { |reactor| Async { reactor.call(batch) } }
          # Reactors return new commands
          commands = runs.flat_map(&:wait)
          Console.info "BATCH #{worker.name} schedulling commands: #{commands.inspect}"

          commands = commands.map { |c| c.with(producer: "worker #{Process.pid}-#{worker.name}") }
          # Put the new commands back in the queue
          worker.call(commands)
        else
          Console.info "Worker #{worker.name} received command: #{command.inspect}"
          Router.handle(command)
        end
      end
    end
  end
end

Console.info 'Bye bye'
# count = 1
# COMMANDS.poll do |command|
#   puts "Received command: id:#{command[:id]} stream_id:#{command[:stream_id]} name:#{command[:data][:name]}"
#   # sleep 1
#   count += 1
#   COMMANDS.stop if count > 100
# end
