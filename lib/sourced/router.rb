# frozen_string_literal: true

require 'singleton'
require 'sourced/injector'

module Sourced
  # The Router is the central dispatch mechanism in Sourced, responsible for:
  # - Registering actors and projectors 
  # - Routing commands to appropriate deciders (actors)
  # - Routing events to appropriate reactors (actors and projectors)
  # - Managing the execution of synchronous and asynchronous reactors
  # - Coordinating with the backend for event storage and retrieval
  #
  # The Router uses the Singleton pattern to ensure a single global registry
  # of all registered components in the system.
  #
  # @example Register components and handle commands
  #   Sourced::Router.register(MyActor)
  #   Sourced::Router.register(MyProjector)
  #   
  #   command = CreateSomething.new(stream_id: 'test', payload: {})
  #   Sourced::Router.handle_command(command)
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

      # Schedule commands for background processing.
      # @param commands [Array<Command>] Commands to schedule
      # @return [void]
      # @see #schedule_commands
      def schedule_commands(commands)
        instance.schedule_commands(commands)
      end

      # Handle a command synchronously by routing it to the appropriate decider.
      # @param command [Command] The command to handle
      # @return [Array] Array containing updated state and new events
      # @see #handle_command
      def handle_command(command)
        instance.handle_command(command)
      end

      # Dispatch the next scheduled command from the backend queue.
      # @return [Boolean] true if a command was dispatched, false if queue is empty
      # @see #dispatch_next_command
      def dispatch_next_command
        instance.dispatch_next_command
      end

      # Handle events by routing them to all registered reactors.
      # @param events [Array<Event>] Events to handle
      # @return [void]
      # @see #handle_events
      def handle_events(events)
        instance.handle_events(events)
      end

      # Get all registered asynchronous reactors.
      # @return [Set] Set of async reactor classes
      # @see #async_reactors
      def async_reactors
        instance.async_reactors
      end

      # Handle and acknowledge events for a specific reactor.
      # @param reactor [Class] The reactor class to handle events for
      # @param events [Array<Event>] Events to handle
      # @return [void]
      # @see #handle_and_ack_events_for_reactor
      def handle_and_ack_events_for_reactor(reactor, events)
        instance.handle_and_ack_events_for_reactor(reactor, events)
      end

      # Handle the next available event for a specific reactor.
      # @param reactor [Class] The reactor class to get events for
      # @param process_name [String, nil] Optional process identifier for logging
      # @return [Boolean] true if an event was handled, false if no events available
      # @see #handle_next_event_for_reactor
      def handle_next_event_for_reactor(reactor, process_name = nil)
        instance.handle_next_event_for_reactor(reactor, process_name)
      end
    end

    # @!attribute [r] sync_reactors
    #   @return [Set] Reactors that run synchronously with command handling
    # @!attribute [r] async_reactors  
    #   @return [Set] Reactors that run asynchronously in background workers
    # @!attribute [r] backend
    #   @return [Object] The configured backend for event storage
    # @!attribute [r] logger
    #   @return [Object] The configured logger instance
    attr_reader :sync_reactors, :async_reactors, :backend, :logger, :kargs_for_handle

    # Initialize a new Router instance.
    # @param backend [Object] Backend for event storage (defaults to configured backend)
    # @param logger [Object] Logger instance (defaults to configured logger)
    def initialize(backend: Sourced.config.backend, logger: Sourced.config.logger)
      @backend = backend
      @logger = logger
      @registered_lookup = {}
      @decider_lookup = {} # <= TODO this is for deprecated command handlers. Remove.
      @kargs_for_handle = {}
      @sync_reactors = Set.new
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

      # Analyze the reactor's #handle method signature and store expected keyword arguments
      # for automatic injection during event processing
      @kargs_for_handle[thing] = Injector.resolve_args(thing, :handle)

      if thing.consumer_info.async
        @async_reactors << thing
      else
        @sync_reactors << thing
      end

      group_id = thing.consumer_info.group_id
      @registered_lookup[group_id] = true
      backend.register_consumer_group(group_id)
    end

    def registered?(thing)
      !!@registered_lookup[thing.consumer_info.group_id]
    end

    # TODO: commands will just be messages now
    # and the #handle_command interface will be removed
    # I need to re-think what sync command handling will look like
    # There's also no constraint that only a single Reactor can handle a message
    # So we either pass the command to a specific reactor, or to all reactors that handle it,
    # and have them all return results, then we pass the results to the backend
    #
    # @param commands [Array<Command>] Commands to schedule for processing
    # @return [void]
    # @raise [UnregisteredCommandError] if no reactor is registered for a command type
    # @example Schedule multiple commands
    #   commands = [
    #     CreateCart.new(stream_id: 'cart-1', payload: {}),
    #     AddItem.new(stream_id: 'cart-1', payload: { product_id: 'p1' })
    #   ]
    #   router.schedule_commands(commands)
    def schedule_commands(commands)
      commands = Array(commands)
      grouped = commands.group_by do |cmd| 
        reactor = @decider_lookup[cmd.class]
        # raise UnregisteredCommandError, "No reactor registered for command #{cmd.class}" unless reactor

        reactor.consumer_info.group_id
      end

      grouped.each do |group_id, cmds|
        backend.schedule_commands(cmds, group_id:)
      end
    end

    # Handle a command synchronously by routing it to the appropriate registered decider.
    # This loads the actor's current state, processes the command, and returns the results.
    #
    # @param command [Command] The command to handle
    # @return [Array] Array containing [updated_state, new_events]
    # @raise [KeyError] if no decider is registered for the command type
    # @example Handle a command directly
    #   command = CreateCart.new(stream_id: 'cart-123', payload: {})
    #   state, events = router.handle_command(command)
    def handle_command(command)
      decider = @decider_lookup.fetch(command.class)
      decider.handle_command(command)
    end

    def handle_events(events)
      event_classes = events.map(&:class)
      reactors = sync_reactors.filter do |r|
        r.handled_events.intersect?(event_classes)
      end
      # TODO
      # Reactors can return commands to run next
      # I need to think about how to best to handle this safely
      # Also this could potential lead to infinite recursion!
      reactors.each do |r|
        handle_and_ack_events_for_reactor(r, events)
      end
    end

    # TODO: re-think "sync mode" based on new #handle(event) => Sourced::Results interface
    #
    # When in sync mode, we want both events
    # and any resulting commands to be processed syncronously
    # and in the same transaction as events are appended to store.
    # We could handle commands in threads or fibers,
    # if they belong to different streams than the events,
    # but we need to make sure to raise exceptions in the main thread.
    # so that the transaction is rolled back.
    def handle_and_ack_events_for_reactor(reactor, events)
      backend.ack_on(reactor.consumer_info.group_id, events.last.id) do
        commands = reactor.handle_events(events)
        if commands && commands.any?
          # TODO: we also need to handle exceptions here
          # TODO2: this is not tested
          commands.each do |cmd|
            log_event(' -> produced command', reactor, cmd)
            handle_command(cmd)
          end
        end
      end
    end

    # TODO: this will be removed.
    # All messages will be handled by #handle_next_event_for_reactor below.
    # We still want to schedule appending messages in the future.
    #
    # These two are invoked by background workers
    # Here we want to handle exceptions
    # and trigger retries if needed
    def dispatch_next_command
      # TODO: I need to get the next available command
      # for a reactor that is ACTIVE
      # so commands in the bus need to know what reactor group_id
      # they belong to
      # OR, on failure I need to mark commands in the bus as failed
      # so that they're ignored by the worker
      # TODO2: actors shouldn't need a backend instance.
      backend.next_command do |cmd|
        reactor = @decider_lookup.fetch(cmd.class)
        reactor.handle_command(cmd, backend:)
        true
      rescue StandardError => e
        logger.warn "[#{PID}]: error handling command #{cmd.class} with reactor #{reactor} #{e}"
        # TODO: if it retries and then succeeds
        # the retry count should be reset
        backend.updating_consumer_group(reactor.consumer_info.group_id) do |group|
          reactor.on_exception(e, cmd, group)
        end
        # Do not remove command
        false
      end
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
    def handle_next_event_for_reactor(reactor, worker_id = nil)
      backend.reserve_next_for_reactor(reactor, worker_id:) do |event, replaying|
        log_event('handling event', reactor, event, worker_id)
        
        # Build keyword arguments hash based on what the reactor's #handle method expects
        kargs = build_reactor_handle_args(reactor, event, replaying)

        # Call the reactor's handle method with the event and any requested keyword arguments
        result = reactor.handle(event, **kargs)
        # TODO2: reactor.handle
        # should return an Array of results
        # including a type to run "sync" operations

        result
      rescue StandardError => e
        logger.warn "[#{PID}]: error handling event #{event.class} with reactor #{reactor} #{e}"
        backend.updating_consumer_group(reactor.consumer_info.group_id) do |group|
          reactor.on_exception(e, event, group)
        end
        # Do not ACK event for reactor
        Results::RETRY
      end
    end

    private

    # Build keyword arguments hash for calling a reactor's #handle method.
    # Only includes arguments that the reactor's method signature actually declares.
    #
    # @param reactor [Class] The reactor class 
    # @param event [Event] The event being processed
    # @param replaying [Boolean] Whether this is a replay operation
    # @return [Hash] Hash of keyword arguments to pass to reactor.handle
    def build_reactor_handle_args(reactor, event, replaying)
      kargs_for_handle[reactor].each.with_object({}) do |name, hash|
        case name
        when :replaying
          hash[name] = replaying
        when :history
          hash[name] = backend.read_event_stream(event.stream_id)
        when :logger
          hash[name] = logger
        end
      end
    end

    def log_event(label, reactor, event, process_name = PID)
      logger.info "[#{process_name}]: #{reactor.consumer_info.group_id} #{label} #{event_info(event)}"
    end

    def event_info(event)
      %([#{event.type}] stream_id:#{event.stream_id} seq:#{event.seq})
    end
  end
end
