# frozen_string_literal: true

require 'singleton'
require 'sourced/injector'

module Sourced
  # The Router is the central dispatch mechanism in Sourced, responsible for:
  # - Registering Reactors (actors and projectors)
  # - Routing events to appropriate reactors
  # - Managing the execution of asynchronous reactors
  # - Coordinating with the backend for event storage and retrieval
  #
  # The Router uses the Singleton pattern to ensure a single global registry
  # of all registered components in the system.
  #
  # @example Register components
  #   Sourced::Router.register(MyActor)
  #   Sourced::Router.register(MyProjector)
  #   
  class Router
    include Singleton

    PID = Process.pid

    class << self
      public :new

      # Register an actor or projector for command/event handling.
      # @param args [Object] Arguments passed to the instance register method
      # @return [void]
      # @see #register
      def register(...)
        instance.register(...)
      end

      # @return [Boolean] true if the class is registered as a decider or reactor
      def registered?(...)
        instance.registered?(...)
      end

      # Get all registered asynchronous reactors.
      # @return [Set] Set of async reactor classes
      # @see #async_reactors
      def async_reactors
        instance.async_reactors
      end

      # Handle the next available event for a specific reactor.
      # @param reactor [Class] The reactor class to get events for
      # @param process_name [String, nil] Optional process identifier for logging
      # @return [Boolean] true if an event was handled, false if no events available
      # @see #handle_next_event_for_reactor
      def handle_next_event_for_reactor(reactor, process_name = nil)
        instance.handle_next_event_for_reactor(reactor, process_name)
      end

      def backend = instance.backend
    end

    # @!attribute [r] async_reactors  
    #   @return [Set] Reactors that run asynchronously in background workers
    # @!attribute [r] backend
    #   @return [Object] The configured backend for event storage
    # @!attribute [r] logger
    #   @return [Object] The configured logger instance
    attr_reader :async_reactors, :backend, :logger, :needs_history

    # Initialize a new Router instance.
    # @param backend [Object] Backend for event storage (defaults to configured backend)
    # @param logger [Object] Logger instance (defaults to configured logger)
    def initialize(backend: Sourced.config.backend, logger: Sourced.config.logger)
      @backend = backend
      @logger = logger
      @registered_lookup = {}
      @needs_history = {}
      @async_reactors = Set.new
    end

    # Register a Reactor with the router.
    # 
    # During registration, the router analyzes the reactor's #handle method signature
    # and stores the expected keyword arguments for automatic injection during event processing.
    # This enables reactors to declare exactly what contextual information they need.
    #
    # @param thing [Class] Reactor object to register.
    # @return [void]
    # @raise [InvalidReactorError] if the class doesn't implement required interfaces
    # 
    # @example Register an actor that handles both commands and events
    #   router.register(CartActor)
    #
    # @example Reactors with different argument requirements
    #   # Reactor that only needs the event
    #   class SimpleReactor
    #     def self.handle(event)
    #       # Process event
    #     end
    #   end
    #
    #   # Reactor that needs to know if it's replaying events
    #   class ReplayAwareReactor  
    #     def self.handle(event, replaying:)
    #       return if replaying  # Skip during replay
    #       # Process event normally
    #     end
    #   end
    #
    #   # Reactor that needs access to full event history
    #   class HistoryReactor
    #     def self.handle(event, history:)
    #       # Analyze event in context of full stream history
    #     end
    #   end
    #
    #   # Reactor that needs both pieces of context
    #   class FullContextReactor
    #     def self.handle(event, replaying:, history:)
    #       return if replaying
    #       # Process with full context
    #     end
    #   end
    def register(thing)
      unless ReactorInterface === thing
        raise InvalidReactorError, "#{thing.inspect} is not a valid Reactor interface"
      end

      # Analyze the reactor's handle_batch signature to determine if it needs history
      @needs_history[thing] = Injector.resolve_args(thing, :handle_batch).include?(:history)
      @async_reactors << thing

      group_id = thing.consumer_info.group_id
      @registered_lookup[group_id] = true
      backend.register_consumer_group(group_id)
    end

    def registered?(thing)
      !!@registered_lookup[thing.consumer_info.group_id]
    end

    # Handle the next available event for a specific reactor with automatic argument injection.
    #
    # This method performs argument injection based on the reactor's #handle method signature
    # that was analyzed during registration. Only the arguments that the reactor actually
    # declared in its method signature will be provided, enabling reactors to opt into
    # exactly the contextual information they need.
    #
    # @param reactor [Class] The reactor class to get events for
    # @param worker_id [String, nil] Optional process identifier for logging
    # @param raise_on_error [Boolean] Raise error immediatly instead of notifying Reactor#on_exception
    # @return [Boolean] true if an event was handled, false if no events available
    #
    # @example Argument injection behavior
    #   # For a reactor with signature: def handle(event)
    #   # Called as: reactor.handle(event)
    #   
    #   # For a reactor with signature: def handle(event, replaying:)
    #   # Called as: reactor.handle(event, replaying: false)  # or true during replay
    #   
    #   # For a reactor with signature: def handle(event, history:) 
    #   # Called as: reactor.handle(event, history: [event1, event2, ...])
    #   
    #   # For a reactor with signature: def handle(event, replaying:, history:)
    #   # Called as: reactor.handle(event, replaying: false, history: [...])
    #
    # Available injectable arguments:
    # - :replaying - Boolean indicating if this is a replay operation
    # - :history - Array of all events in the stream up to this point
    def handle_next_event_for_reactor(reactor, worker_id = nil, raise_on_error = false, batch_size: 1)
      found = false

      backend.reserve_next_for_reactor(reactor, batch_size:, with_history: @needs_history[reactor], worker_id:) do |batch, history|
        found = true
        first_msg = batch.first&.first
        log_event('handling batch', reactor, first_msg, worker_id) if first_msg

        kargs = {}
        kargs[:history] = history if @needs_history[reactor]
        reactor.handle_batch(batch, **kargs)
      rescue StandardError => e
        raise e if raise_on_error

        logger.warn "[#{PID}]: error handling batch with reactor #{reactor} #{e}"
        backend.updating_consumer_group(reactor.consumer_info.group_id) do |group|
          reactor.on_exception(e, batch.first&.first, group)
        end
        Actions::RETRY
      end
      found
    end

    # Handle messages for reactors in this router
    # until there's none left in the backend
    # Useful for testing workflows
    # @param limit [Numeric] How many times to loop fetching new messages
    def drain(limit = Float::INFINITY)
      pid = Process.pid
      have_messages = @async_reactors.each.with_index.with_object({}) { |(_, i), m| m[i] = true }

      count = 0
      loop do
        count += 1
        @async_reactors.each.with_index do |r, idx|
          found = handle_next_event_for_reactor(r, pid, true)
          have_messages[idx] = found
        end
        break if have_messages.values.none? || count >= limit
      end
    end

    private

    def log_event(label, reactor, event, process_name = PID)
      logger.info "[#{process_name}]: #{reactor.consumer_info.group_id} #{label} #{event_info(event)}"
    end

    def event_info(event)
      %([#{event.type}] stream_id:#{event.stream_id} seq:#{event.seq})
    end
  end
end
