# frozen_string_literal: true

require 'console' #  comes with Async
require 'sors/machine' #  comes with Async
require 'sors/router' #  comes with Async

module Sors
  class Worker
    attr_reader :name

    def initialize(backend, logger:, name: SecureRandom.hex(4), poll_interval: 0.01)
      @backend = backend
      @logger = logger
      @running = false
      @name = [Process.pid, name].join('-')
      @poll_interval = poll_interval
    end

    def stop
      @running = false
    end

    def poll
      poller = method(:work)
      @running = true
      while @running
        @backend.reserve_next(&poller)
        # This sleep seems to be necessary or workers in differet processes will not be able to get the lock
        sleep @poll_interval
      end
      logger.info "Worker #{name}: Polling stopped"
    end

    def work(command)
      case command
      when Machine::ProcessBatch
        logger.warn "BATCH #{name} received command: #{command.type}"
        batch = @backend.read_event_batch(command.causation_id)
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
        # TODO: handle reactor errors
        runs = reactors.map { |reactor| Async { reactor.call(batch) } }
        # Reactors return new commands
        commands = runs.flat_map(&:wait)
        logger.info "BATCH #{name} schedulling commands: #{commands.map(&:type).inspect}"

        commands = commands.map { |c| c.with(producer: "worker #{name}") }
        # Put the new commands back in the queue
        @backend.schedule_commands(commands)
      else
        logger.info "Worker #{name} received command: #{command.type}"
        Router.handle(command)
      end
    end

    private

    attr_reader :logger
  end
end

# require_relative 'test'
#
# THREADS = 4
# WORKERS = THREADS.times.map do |i|
#   Commander.new(DB, i)
# end
#
# trap('INT') { WORKERS.each(&:stop) }
#
# Sync do
#   WORKERS.map do |worker|
#     Async do
#       worker.poll do |command|
#         case command
#         when Machine::ProcessBatch
#           Console.warn "BATCH #{worker.name} received command: #{command.inspect}"
#           batch = ES.read_batch(command.causation_id)
#           reactors = Router.reactors_for(batch)
#           # TODO: reactors should work concurrently
#           # either using a thread/fiber pool here, or by scheduling
#           # execution as individual commands
#           # Or by passing reactors to other threads in this group
#           # However, if a reactor fails in a thread, this block will be unaware
#           # and the original batch command will be lost
#           # Perhaps: Machine::RunReactor.new(reactor_name, batch_id)
#
#           # Run this batch of events through any reactors
#           # that are interested in them
#           # Each reactor runs in a separate Fiber
#           runs = reactors.map { |reactor| Async { reactor.call(batch) } }
#           # Reactors return new commands
#           commands = runs.flat_map(&:wait)
#           Console.info "BATCH #{worker.name} schedulling commands: #{commands.inspect}"
#
#           commands = commands.map { |c| c.with(producer: "worker #{Process.pid}-#{worker.name}") }
#           # Put the new commands back in the queue
#           worker.call(commands)
#         else
#           Console.info "Worker #{worker.name} received command: #{command.inspect}"
#           Router.handle(command)
#         end
#       end
#     end
#   end
# end
#
# Console.info 'Bye bye'
# count = 1
# COMMANDS.poll do |command|
#   puts "Received command: id:#{command[:id]} stream_id:#{command[:stream_id]} name:#{command[:data][:name]}"
#   # sleep 1
#   count += 1
#   COMMANDS.stop if count > 100
# end
