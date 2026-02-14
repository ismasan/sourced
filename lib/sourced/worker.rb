# frozen_string_literal: true

require 'console' #  comes with Async
require 'sourced/router' #  comes with Async

module Sourced
  # A Worker is responsible for processing messages in the background.
  # Workers poll registered async reactors for available messages and dispatch
  # them to reactors in a round-robin fashion.
  #
  # Workers use a polling model with configurable intervals and can be run
  # in multiple processes for horizontal scaling. Each worker maintains its
  # own set of reactors and processes them independently.
  #
  # @example Create and start a worker
  #   worker = Sourced::Worker.new(name: 'worker-1')
  #   worker.poll  # Start polling for work
  #
  class Worker
    DEFAULT_SHUFFLER = ->(array) { array.shuffle }

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
      router: Sourced::Router,
      shuffler: DEFAULT_SHUFFLER,
      batch_size: 1
    )
      @logger = logger
      @running = false
      @name = [Process.pid, name].join('-')
      @poll_interval = poll_interval
      @router = router
      @batch_size = batch_size
      # TODO: If reactors have a :weight, we can use that
      # to populate this array according to the weight
      # so that some reactors are picked more often than others
      @reactors = shuffler.call(@router.async_reactors.filter { |r| r.handled_messages.any? }.to_a)
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
    # This method will block and process messages in a loop
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
      end
      logger.info "Worker #{name}: Polling stopped"
    end

    # Process one tick of work for a specific reactor.
    # If no reactor is specified, uses the next reactor in round-robin order.
    #
    # @param reactor [Class, nil] Specific reactor to process (defaults to next in rotation)
    # @return [Boolean] true if an event was processed, false otherwise
    def tick(reactor = next_reactor)
      @router.handle_next_event_for_reactor(reactor, name, batch_size: @batch_size)
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
