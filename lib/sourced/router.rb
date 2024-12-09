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

      raise InvalidReactorError, "#{thing.inspect} is not a valid Decider or Reactor interface" unless regs.positive?
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

    def handle_next_event_for_reactor(reactor, process_name = nil)
      backend.reserve_next_for_reactor(reactor) do |event|
        # We're dealing with one event at a time now
        # So reactors should return a single command, or nothing
        log_event('handling event', reactor, event, process_name)
        commands = reactor.handle_events([event])
        if commands.any?
          # This will run a new decider
          # which may be expensive, timeout, or raise an exception
          # TODO: handle decider errors
          backend.schedule_commands(commands)
        end

        event
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
          # TODO: Commands may or may not belong to he same stream as events
          # if they belong to the same stream,
          # hey need to be dispached in order to preserve per stream order
          # If they belong to different streams, they can be dispatched in parallel
          # or put in a command bus.
          # TODO2: we also need to handle exceptions here
          # TODO3: this is not tested
          commands.each do |cmd|
            log_event(' -> produced command', reactor, cmd)
            handle_command(cmd)
          end
        end
      end
    end

    def dispatch_next_command
      backend.next_command do |cmd|
        #  TODO: error handling
        handle_command(cmd)
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
