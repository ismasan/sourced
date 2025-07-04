# frozen_string_literal: true

require 'singleton'

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
    # Raised when attempting to handle a command with no registered handler
    UnregisteredCommandError = Class.new(StandardError)

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
    attr_reader :sync_reactors, :async_reactors, :backend, :logger

    # Initialize a new Router instance.
    # @param backend [Object] Backend for event storage (defaults to configured backend)
    # @param logger [Object] Logger instance (defaults to configured logger)
    def initialize(backend: Sourced.config.backend, logger: Sourced.config.logger)
      @backend = backend
      @logger = logger
      @registered_lookup = {}
      @decider_lookup = {}
      @sync_reactors = Set.new
      @async_reactors = Set.new
    end

    # Register an actor or projector with the router.
    # Components implementing DeciderInterface can handle commands.
    # Components implementing ReactorInterface can handle events.
    # A single class can implement both interfaces.
    #
    # @param thing [Class] Actor or Projector class to register
    # @return [void]
    # @raise [InvalidReactorError] if the class doesn't implement required interfaces
    # @example Register an actor that handles both commands and events
    #   router.register(CartActor)
    # @example Register a projector that only handles events
    #   router.register(CartListingsProjector)
    def register(thing)
      regs = 0
      if DeciderInterface === thing
        regs += 1
        thing.handled_commands.each do |cmd_type|
          @decider_lookup[cmd_type] = thing
        end
      end

      if ReactorInterface === thing
        regs += 1

        if thing.consumer_info.async
          @async_reactors << thing
        else
          @sync_reactors << thing
        end
      end

      if regs.positive?
        group_id = thing.consumer_info.group_id
        @registered_lookup[group_id] = true
        backend.register_consumer_group(group_id)
      else
        raise InvalidReactorError, "#{thing.inspect} is not a valid Decider or Reactor interface"
      end
    end

    def registered?(thing)
      !!@registered_lookup[thing.consumer_info.group_id]
    end

    # Schedule commands for background processing by registered actors.
    # Commands are grouped by their target reactor's consumer group ID to ensure
    # proper ordering and that only commands for active reactors are fetched.
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
        raise UnregisteredCommandError, "No reactor registered for command #{cmd.class}" unless reactor

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
      backend.next_command do |cmd|
        reactor = @decider_lookup.fetch(cmd.class)
        reactor.handle_command(cmd)
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

    def handle_next_event_for_reactor(reactor, worker_id = nil)
      backend.reserve_next_for_reactor(reactor, worker_id:) do |event, replaying|
        log_event('handling event', reactor, event, worker_id)
        commands = reactor.handle_events([event], replaying:)
        if commands.any?
          # TODO: this schedules commands that will be picked up
          # by #dispatch_next_command above on the worker's next tick
          schedule_commands(commands)
        end

        event
      rescue UnregisteredCommandError
        raise
      rescue StandardError => e
        logger.warn "[#{PID}]: error handling event #{event.class} with reactor #{reactor} #{e}"
        backend.updating_consumer_group(reactor.consumer_info.group_id) do |group|
          reactor.on_exception(e, event, group)
        end
        # Do not ACK event for reactor
        false
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
