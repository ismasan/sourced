# frozen_string_literal: true

require 'singleton'

module Sourced
  class Router
    include Singleton

    PID = Process.pid

    class << self
      public :new

      def register(...)
        instance.register(...)
      end

      def schedule_commands(commands)
        instance.schedule_commands(commands)
      end

      def handle_command(command)
        instance.handle_command(command)
      end

      def dispatch_next_command
        instance.dispatch_next_command
      end

      def handle_events(events)
        instance.handle_events(events)
      end

      def async_reactors
        instance.async_reactors
      end

      def handle_and_ack_events_for_reactor(reactor, events)
        instance.handle_and_ack_events_for_reactor(reactor, events)
      end

      def handle_next_event_for_reactor(reactor, process_name = nil)
        instance.handle_next_event_for_reactor(reactor, process_name)
      end
    end

    attr_reader :sync_reactors, :async_reactors, :backend, :logger

    def initialize(backend: Sourced.config.backend, logger: Sourced.config.logger)
      @backend = backend
      @logger = logger
      @decider_lookup = {}
      @sync_reactors = Set.new
      @async_reactors = Set.new
    end

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
        backend.register_consumer_group(thing.consumer_info.group_id)
      else
        raise InvalidReactorError, "#{thing.inspect} is not a valid Decider or Reactor interface"
      end
    end

    # Schedule commands for later processing
    # commands are scheduled with the group_id or their target reactor
    # So that we only fetch the next available commands for ACTIVE reactors.
    # @param [Array<Sourced::Message>] commands
    def schedule_commands(commands)
      commands = Array(commands)
      grouped = commands.group_by { |cmd| @decider_lookup.fetch(cmd.class).consumer_info.group_id }
      grouped.each do |group_id, cmds|
        backend.schedule_commands(cmds, group_id:)
      end
    end

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
        #  TODO: handle exceptions here
        # handle_command(cmd)
        reactor = @decider_lookup.fetch(cmd.class)
        # backend.with_context_for(reactor) do |ctx|
        reactor.handle_command(cmd)
        # end
      rescue StandardError => e
        logger.warn "[#{PID}]: error handling command #{cmd.class} with reactor #{reactor} #{e}"
        # TODO: if it retries and then succeeds
        # the retry count should be reset
        # TODO: if an exception is raised
        # we don't want to crash the worker
        # but we also DO NOT WANT TO ACK THIS COMMAND
        # Ie it should not be removed from the command bus
        # Same for #handle_next_event_for_reactor() below
        backend.updating_consumer_group(reactor.consumer_info.group_id) do |ctx|
          reactor.on_exception(e, cmd, ctx)
        end
        # Do not remove command
        false
      end
    end

    def handle_next_event_for_reactor(reactor, process_name = nil)
      backend.reserve_next_for_reactor(reactor) do |event|
        log_event('handling event', reactor, event, process_name)
        #  TODO: handle exceptions here
        commands = reactor.handle_events([event])
        if commands.any?
          # TODO: this schedules commands that will be picked up
          # by #dispatch_next_command above on the worker's next tick
          schedule_commands(commands)
        end

        event
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
