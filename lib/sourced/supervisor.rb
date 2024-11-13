# frozen_string_literal: true

require 'async'
require 'console'
require 'sourced/worker'

module Sourced
  class Supervisor
    def self.start(...)
      new(...).start
    end

    def initialize(backend: Sourced.config.backend, logger: Sourced.config.logger, count: 2)
      @backend = backend
      @logger = logger
      @count = count
      @workers = []
    end

    def start
      logger.info("Starting sync supervisor with #{@count} workers")
      set_signal_handlers
      @workers = @count.times.map do |i|
        Worker.new(backend: @backend, logger:, name: "worker-#{i}")
      end
      Sync do |task|
        @workers.each do |wrk|
          task.async do
            wrk.poll
          end
        end
      end
    end

    def stop
      logger.info("Stopping #{@workers.size} workers")
      @workers.each(&:stop)
      logger.info('All workers stopped')
    end

    def set_signal_handlers
      Signal.trap('INT') { stop }
      Signal.trap('TERM') { stop }
    end

    private

    attr_reader :logger
  end
end
