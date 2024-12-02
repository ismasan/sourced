# frozen_string_literal: true

require 'console' #  comes with Async
require 'sourced/router' #  comes with Async

module Sourced
  class Worker
    def self.drain
      new.drain
    end

    def self.tick
      new.tick
    end

    attr_reader :name

    def initialize(
      logger: Sourced.config.logger,
      name: SecureRandom.hex(4),
      poll_interval: 0.01
    )
      @logger = logger
      @running = false
      @name = [Process.pid, name].join('-')
      @poll_interval = poll_interval
      # TODO: If reactors have a :weight, we can use that
      # to populate this array according to the weight
      # so that some reactors are picked more often than others
      @reactors = Router.async_reactors.filter do |r|
        r.handled_events.any?
      end.to_a.shuffle
      @reactor_index = 0
    end

    def stop
      @running = false
    end

    def poll
      if @reactors.empty?
        logger.warn "Worker #{name}: No reactors to poll"
        return false
      end

      @running = true
      while @running
        tick
        # This sleep seems to be necessary or workers in differet processes will not be able to get the lock
        sleep @poll_interval
      end
      logger.info "Worker #{name}: Polling stopped"
    end

    # Drain all reactors
    def drain
      @reactors.each do |reactor|
        loop do
          event = tick(reactor)
          break unless event
        end
      end
    end

    def tick(reactor = next_reactor)
      Router.handle_next_event_for_reactor(reactor, name)
    end

    def next_reactor
      @reactor_index = 0 if @reactor_index >= @reactors.size
      reactor = @reactors[@reactor_index]
      @reactor_index += 1
      reactor
    end

    private

    attr_reader :logger
  end
end
