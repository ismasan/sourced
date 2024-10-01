# frozen_string_literal: true

require 'async'
require 'console'
require 'sors/worker'

module Sors
  class Supervisor
    def initialize(backend: Sors.config.backend, logger: Sors.config.logger, count: 2)
      @backend = backend
      @logger = logger
      @count = count
      @workers = []
      @tasks = []
    end

    def start
      logger.info("Starting sync supervisor with #{@count} workers")
      set_signal_handlers
      @workers = @count.times.map do |i|
        Worker.new(@backend, logger:, name: "worker-#{i}")
      end
      Sync do
        @tasks = @workers.map do |wrk|
          Async do
            wrk.poll
          end
        end
      end
      logger.info('All workers stopped')
    end

    def wait
      @tasks.map(&:wait)
    end

    def stop
      logger.info("Stopping #{@workers.size} workers")
      @workers.each(&:stop)
      wait
    end

    def set_signal_handlers
      Signal.trap('INT') { stop }
      Signal.trap('TERM') { stop }
    end

    private

    attr_reader :logger
  end
end
