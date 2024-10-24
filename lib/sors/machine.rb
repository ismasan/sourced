# frozen_string_literal: true

module Sors
  class Machine
    include Decide
    include Evolve
    include React
    include ReactSync

    attr_reader :backend

    class << self
      attr_reader :state_factory

      # Register as a Reactor
      def handled_events = self.handled_events_for_react

      # The Reactor interface
      # @param events [Array<Message>]
      def handle_events(events)
        new.handle_events(events)
      end

      # The Decider interface
      # @param cmd [Message]
      def handle_command(cmd)
        new.handle_command(cmd)
      end

      # Define a factory for initial state
      # Example:
      #   state { |stream_id| MyState.new(stream_id) }
      #
      # @param callable [#call, nil] a callable that returns a new state
      # @yieldparam stream_id [String] the stream id
      def state(callable = nil, &block)
        st = callable || block
        raise ArgumentError, 'state must be a callable' unless st.respond_to?(:call)

        @state_factory = st
      end
    end

    attr_reader :seq

    def initialize(logger: Sors.config.logger, backend: Sors.config.backend, state_factory: self.class.state_factory)
      @logger = logger
      @backend = backend
      @seq = 0
      @state_factory = state_factory
      raise ArgumentError, 'state_factory must be a callable' unless @state_factory.respond_to?(:call)
    end

    def inspect
      %(<#{self.class}:#{object_id} backend:#{backend.inspect}>)
    end

    def ==(other)
      other.is_a?(self.class) && other.backend == backend
    end

    def new_state(stream_id)
      @state_factory.call(stream_id)
    end

    def handle_command(command)
      logger.info "Handling #{command.type}"
      state = load(command.stream_id)
      events = decide(state, command)
      state = evolve(state, events)
      transaction do
        events = save(state, command, events)
        # handle sync reactors here
        commands = react_sync(state, events)
        # Schedule a system command to handle this batch of events in the background
        schedule_batch(command, commands)
        # schedule_commands(commands)
      end
      [ state, events ]
    end

    # Reactor interface
    def handle_events(events, &map_commands)
      state = load(events.first.stream_id)
      commands = react(state, events)
      commands = commands.map(&map_commands) if map_commands
      schedule_commands(commands)
    end

    private

    attr_reader :logger

    def load(stream_id)
      state = new_state(stream_id)
      events = backend.read_event_stream(stream_id)
      @seq = events.last&.seq || 0
      evolve(state, events)
    end

    def save(state, command, events)
      # Update :seq for each event based on seq
      # TODO: we do the same in Aggregate#save. DRY this up
      events = [command, *events].map do |event|
        @seq += 1
        event.with(seq: @seq)
      end
      Sors.config.logger.info "Persisting #{state}, #{events} to #{backend.inspect}"
      backend.append_events(events)
      events
    end

    def schedule_batch(command, commands)
      schedule_commands([command.follow(ProcessBatch), *commands])
    end

    def schedule_commands(commands)
      backend.schedule_commands(commands)
    end

    def transaction(&)
      backend.transaction(&)
    end
  end
end
