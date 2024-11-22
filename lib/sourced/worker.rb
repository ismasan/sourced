# frozen_string_literal: true

require 'console' #  comes with Async
require 'sourced/router' #  comes with Async

module Sourced
  class Worker
    def self.drain
      new(async: false).drain
    end

    def self.tick
      new(async: false).tick
    end

    attr_reader :name

    def initialize(
      backend: Sourced.config.backend,
      logger: Sourced.config.logger,
      name: SecureRandom.hex(4),
      poll_interval: 0.01,
      async: true
    )
      @backend = backend
      @logger = logger
      @running = false
      @name = [Process.pid, name].join('-')
      @poll_interval = poll_interval
      # TODO: If reactors have a :weight, we can use that
      # to populate this array according to the weight
      # so that some reactors are picked more often than others
      @reactors = Router.reactors.filter do |r|
        r.handled_events.any?
      end.to_a.shuffle
      @reactor_index = 0
    end

    def stop
      @running = false
    end

    def poll
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
      # @backend.reserve_next_for(reactor.consumer_info.group_id) do |event|
      # TODO: if reactor has empty .handled_events
      # and it doesn't handle :any events, it should be skipped
      @backend.reserve_next_for_reactor(reactor) do |event|
        # We're dealing with one event at a time now
        # So reactors should return a single command, or nothing
        log_event('handling event', reactor, event)
        commands = reactor.handle_events([event])
        if commands.any?
          # This will run a new decider
          # which may be expensive, timeout, or raise an exception
          # TODO: handle decider errors
          # TODO2: this breaks the per-stream concurrency model.
          # Ex. if the current event belongs to a 'cart1' stream,
          # the DB is locked from processing any new events for 'cart1'
          # until we exist this block.
          # if ex. reactor is a Saga that produces a command for another stream
          # (ex. 'mailer-1'), by processing the command here inline, we're blocking
          # the 'cart1' stream unnecessarily
          # Instead, we can:
          # if command.stream_id == event.stream_id
          # * it's Ok to block, as we keep per-stream concurrency
          # if command.stream_id != event.stream_id
          # * we should schedule the command to be picked up later.
          # We can't just run the command in a separate Fiber.
          # We want the durability of a command bus.
          # A command bus will also solve future and recurrent scheduled commands.
          log_event(' -> produced command', reactor, commands.first)
          Sourced::Router.handle_command(commands.first)
        end

        event
      end
    end

    def log_event(label, reactor, event)
      logger.info "[#{name}]: #{reactor.consumer_info.group_id} #{label} #{event_info(event)}"
    end

    def event_info(event)
      %([#{event.type}] stream_id:#{event.stream_id} seq:#{event.seq})
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
