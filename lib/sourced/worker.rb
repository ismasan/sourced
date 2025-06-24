# frozen_string_literal: true

require 'console' #  comes with Async
require 'sourced/router' #  comes with Async

module Sourced
  # A Worker is responsible for processing events and commands in the background.
  # Workers poll registered async reactors for available events and dispatch
  # scheduled commands in a round-robin fashion.
  #
  # Workers use a polling model with configurable intervals and can be run
  # in multiple processes for horizontal scaling. Each worker maintains its
  # own set of reactors and processes them independently.
  #
  # @example Create and start a worker
  #   worker = Sourced::Worker.new(name: 'worker-1')
  #   worker.poll  # Start polling for work
  #
  # @example Drain all pending work
  #   Sourced::Worker.drain
  class Worker
    # Drain all pending work using a new worker instance.
    # This processes all available events and commands until queues are empty.
    #
    # @return [void]
    def self.drain
      new.drain
    end

    # Process one tick of work using a new worker instance.
    # @return [Boolean] true if work was processed, false otherwise
    def self.tick
      new.tick
    end

    # @!attribute [r] name
    #   @return [String] Unique identifier for this worker instance
    attr_reader :name

    # Initialize a new worker instance.
    # The worker will automatically discover and shuffle async reactors
    # to ensure fair distribution of work across multiple worker instances.
    #
    # @param logger [Object] Logger instance for worker output (defaults to configured logger)
    # @param name [String] Unique name for this worker (defaults to random hex)
    # @param poll_interval [Float] Seconds to sleep between polling cycles (defaults to 0.01)
    def initialize(
      logger: Sourced.config.logger,
      name: SecureRandom.hex(4),
      poll_interval: 0.01,
      router: Sourced::Router
    )
      @logger = logger
      @running = false
      @name = [Process.pid, name].join('-')
      @poll_interval = poll_interval
      @router = router
      # TODO: If reactors have a :weight, we can use that
      # to populate this array according to the weight
      # so that some reactors are picked more often than others
      @reactors = @router.async_reactors.filter do |r|
        r.handled_events.any? || r.handled_commands.any?
      end.to_a.shuffle
      @reactor_index = 0
    end

    # Signal the worker to stop polling.
    # The worker will finish its current cycle and then stop.
    #
    # @return [void]
    def stop
      @running = false
    end

    # Start polling for work continuously until stopped.
    # This method will block and process events and commands in a loop
    # until {#stop} is called.
    #
    # @return [void]
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
        dispatch_next_command.tap do |c|
          sleep @poll_interval if c
        end
      end
      logger.info "Worker #{name}: Polling stopped"
    end

    # Drain all pending work from reactors and command queues.
    # This processes all available events for each reactor and then
    # handles all scheduled commands until the queues are empty.
    #
    # @return [void]
    def drain
      @reactors.each do |reactor|
        loop do
          event = tick(reactor)
          break unless event
        end
      end

      loop do
        cmd = dispatch_next_command
        break unless cmd
      end
    end

    # Process one tick of work for a specific reactor.
    # If no reactor is specified, uses the next reactor in round-robin order.
    #
    # @param reactor [Class, nil] Specific reactor to process (defaults to next in rotation)
    # @return [Boolean] true if an event was processed, false otherwise
    def tick(reactor = next_reactor)
      @router.handle_next_event_for_reactor(reactor, name)
    end

    # Dispatch the next scheduled command from the backend queue.
    #
    # @return [Boolean] true if a command was dispatched, false if queue is empty
    def dispatch_next_command
      @router.dispatch_next_command
    end

    # Get the next reactor in round-robin order.
    # Cycles through all available async reactors to ensure fair processing.
    #
    # @return [Class] Next reactor class to process
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
