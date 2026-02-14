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
      def handle_next_event_for_reactor(...)
        instance.handle_next_event_for_reactor(...)
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
    # During registration, the router analyzes the reactor's #handle_batch method signature
    # to determine whether it needs stream history. Reactors that declare a `history:` keyword
    # will receive the full stream history when processing batches.
    #
    # @param thing [Class] Reactor object to register.
    # @return [void]
    # @raise [InvalidReactorError] if the class doesn't implement required interfaces
    #
    # @example Register an actor that handles both commands and events
    #   router.register(CartActor)
    #
    # @example Reactors with different handle_batch signatures
    #   # Reactor that doesn't need history (default Consumer wrapper)
    #   class SimpleReactor
    #     extend Sourced::Consumer
    #     def self.handle(event) = Sourced::Actions::OK
    #   end
    #
    #   # Reactor that needs access to full stream history
    #   class HistoryReactor
    #     extend Sourced::Consumer
    #     def self.handle_batch(batch, history:)
    #       # batch is Array of [message, replaying] pairs
    #       # history is Array of all messages in the stream
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

    # Handle the next available batch of messages for a specific reactor.
    #
    # Fetches a batch of messages from the backend and calls reactor.handle_batch(batch, **kargs).
    # If the reactor's handle_batch signature includes `history:`, the full stream history
    # is fetched from the backend and passed through.
    #
    # @param reactor [Class] The reactor class to get events for
    # @param worker_id [String, nil] Optional process identifier for logging
    # @param raise_on_error [Boolean] Raise error immediately instead of notifying Reactor#on_exception
    # @param batch_size [Integer] Number of messages to fetch per lock cycle
    # @return [Boolean] true if a batch was handled, false if no messages available
    def handle_next_event_for_reactor(reactor, worker_id = nil, raise_on_error = false, batch_size: 1)
      effective_batch_size = reactor.consumer_info.batch_size || batch_size
      found = false

      backend.reserve_next_for_reactor(reactor, batch_size: effective_batch_size, with_history: @needs_history[reactor], worker_id:) do |batch, history|
        found = true
        first_msg = batch.first&.first
        log_event("handling batch(#{batch.size})", reactor, first_msg, worker_id) if first_msg

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
