# frozen_string_literal: true

require 'async'
require 'console'
require 'sors/worker'

module Sors
  class Supervisor
    def initialize(backend:, count: 2)
      @backend = backend
      @count = count
      @workers = []
      @tasks = []
    end

    def start
      Console.info("Starting sync supervisor with #{@count} workers")
      set_signal_handlers
      @workers = @count.times.map do |i|
        Worker.new(@backend, name: "worker-#{i}")
      end
      Sync do
        @tasks = @workers.map do |wrk|
          Async do
            wrk.poll
          end
        end
      end
      Console.info('All workers stopped')
    end

    def wait
      @tasks.map(&:wait)
    end

    def stop
      Console.info("Stopping #{@workers.size} workers")
      @workers.each(&:stop)
      wait
    end

    def set_signal_handlers
      Signal.trap('INT') { stop }
      Signal.trap('TERM') { stop }
    end
  end
end
