# frozen_string_literal: true

require 'sourced/injector'

module Sourced
  # Executes a group of reactors synchronously within a single transaction,
  # using breadth-first traversal of the message graph.
  #
  # A Unit wires multiple reactors (actors, projectors) together so that
  # the full command -> event -> reaction -> command chain runs in one
  # backend transaction, giving all-or-nothing semantics. Messages
  # produced by one reactor are immediately routed to any other reactor
  # in the unit that handles them.
  #
  # @example Basic usage with two actors
  #   unit = Sourced::Unit.new(OrderActor, PaymentActor, backend: backend)
  #   results = unit.handle(PlaceOrder.new(stream_id: 'order-1', payload: { amount: 100 }))
  #
  #   results.events_for(OrderActor)   # => [OrderPlaced, ...]
  #   results.events_for(PaymentActor) # => [PaymentCharged, ...]
  #
  # @example Skip persisting commands (events-only storage)
  #   unit = Sourced::Unit.new(OrderActor, backend: backend, persist_commands: false)
  #   unit.handle(cmd) # only events are written to the store
  #
  # @example Limit BFS depth to detect runaway loops
  #   unit = Sourced::Unit.new(LoopyActor, backend: backend, max_iterations: 10)
  #   unit.handle(cmd) # raises Sourced::Unit::InfiniteLoopError if chain exceeds 10 steps
  class Unit
    # Default cap on BFS iterations before raising {InfiniteLoopError}.
    DEFAULT_MAX_ITERATIONS = 100

    # Raised when the BFS message loop exceeds {#max_iterations}.
    InfiniteLoopError = Class.new(Sourced::Error)

    # @param reactors [Array<Class>] reactor classes (actors, projectors) to wire together
    # @param backend [#transaction, #append_to_stream, #append_next_to_stream] storage backend
    # @param logger [#info, #debug] logger instance
    # @param max_iterations [Integer] safety cap on BFS iterations
    # @param persist_commands [Boolean] when false, only events are written to the store
    def initialize(*reactors, backend: Sourced.config.backend, logger: Sourced.config.logger,
                   max_iterations: DEFAULT_MAX_ITERATIONS, persist_commands: true)
      @reactors = reactors
      @backend = backend
      @logger = logger
      @max_iterations = max_iterations
      @persist_commands = persist_commands
      @routing_table = build_routing_table
      @kargs_for_handle = resolve_kargs
    end

    # Run the full message chain for +initial_message+ inside a single transaction.
    #
    # Persists the initial message (subject to +persist_commands+), then
    # breadth-first routes every produced message to matching reactors until
    # no more messages are enqueued. Finally, ACKs all handled messages so
    # background workers won't re-process them.
    #
    # @param initial_message [Sourced::Message] the command or event that kicks off the chain
    # @return [Results] per-reactor instances and produced events
    # @raise [InfiniteLoopError] if BFS iterations exceed +max_iterations+
    #
    # @example
    #   unit = Sourced::Unit.new(MyActor, backend: backend)
    #   results = unit.handle(MyCommand.new(stream_id: 'abc', payload: { name: 'test' }))
    #   results.events_for(MyActor) # => [MyEvent, ...]
    def handle(initial_message)
      results = Results.new
      queue = [initial_message]
      iteration = 0

      @backend.transaction do
        ensure_consumer_groups!

        if should_persist?(initial_message)
          @backend.append_next_to_stream(initial_message.stream_id, [initial_message])
        end

        while (message = queue.shift)
          iteration += 1
          raise InfiniteLoopError, "Exceeded #{@max_iterations} iterations" if iteration > @max_iterations

          handlers = @routing_table[message.class] || []
          handlers.each do |reactor_class|
            new_messages = handle_for_reactor(reactor_class, message, results)
            queue.concat(new_messages)
          end
        end

        ack_all(results)
      end

      results
    end

    private

    # Whether +message+ should be written to the event store.
    # Events are always persisted; commands are only persisted
    # when +@persist_commands+ is true.
    #
    # @param message [Sourced::Message]
    # @return [Boolean]
    def should_persist?(message)
      return true if @persist_commands

      message.is_a?(Sourced::Event)
    end

    # Instantiate a reactor, replay its history, handle the message,
    # and process resulting actions. Returns messages that should
    # continue the BFS traversal.
    #
    # @param reactor_class [Class] the reactor class to handle the message
    # @param message [Sourced::Message] the message to handle
    # @param results [Results] accumulator for instances and events
    # @return [Array<Sourced::Message>] new messages to enqueue for BFS
    def handle_for_reactor(reactor_class, message, results)
      instance = reactor_class.new(id: reactor_class.identity_from(message))
      history = @backend.read_stream(message.stream_id)

      kargs = build_handle_args(reactor_class, history)
      actions = instance.handle(message, **kargs)

      new_messages, produced_events = process_actions(message, actions)

      results.record(reactor_class, instance, produced_events, message)

      new_messages
    end

    # Build keyword arguments for the reactor's #handle method
    # based on which parameters it declares (replaying, history, logger).
    #
    # @param reactor_class [Class]
    # @param history [Array<Sourced::Message>]
    # @return [Hash]
    def build_handle_args(reactor_class, history)
      @kargs_for_handle[reactor_class].each_with_object({}) do |name, hash|
        case name
        when :replaying
          hash[name] = false
        when :history
          hash[name] = history
        when :logger
          hash[name] = @logger
        end
      end
    end

    # Execute a list of actions produced by a reactor.
    #
    # {Actions::AppendAfter} and {Actions::AppendNext} are handled explicitly
    # because the Unit needs their correlated messages for BFS traversal and
    # produced-events tracking. {Actions::AppendNext} also respects
    # +persist_commands+. {Actions::Schedule} and {Actions::Sync} delegate
    # to {Actions::Schedule#execute} / {Actions::Sync#execute}.
    #
    # @param source_message [Sourced::Message] message that triggered the actions
    # @param actions [Array<Object>, Object] action(s) returned by the reactor
    # @return [Array(Array<Sourced::Message>, Array<Sourced::Message>)]
    #   a two-element array: [new_messages_for_bfs, produced_events]
    def process_actions(source_message, actions)
      new_messages = []
      produced_events = []
      actions = [actions] unless actions.is_a?(Array)
      actions = actions.compact

      actions.each do |action|
        case action
        when Actions::AppendAfter
          messages = action.execute(@backend, source_message)
          produced_events.concat(messages)
          new_messages.concat(messages.select { |m| handled_by_unit?(m) })

        when Actions::AppendNext
          messages = action.messages.map { |m| source_message.correlate(m) }
          messages.group_by(&:stream_id).each do |stream_id, stream_messages|
            if should_persist?(stream_messages.first)
              @backend.append_next_to_stream(stream_id, stream_messages)
            end
          end
          new_messages.concat(messages.select { |m| handled_by_unit?(m) })

        when Actions::Schedule, Actions::Sync
          action.execute(@backend, source_message)

        when Actions::OK, :ok
          # no-op
        end
      end

      [new_messages, produced_events]
    end

    # Whether the message type is handled by any reactor in this unit.
    #
    # @param message [Sourced::Message]
    # @return [Boolean]
    def handled_by_unit?(message)
      @routing_table.key?(message.class)
    end

    # ACK every message that was handled during this unit of work
    # so background workers won't re-process them.
    #
    # @param results [Results]
    def ack_all(results)
      results.each_ack do |group_id, message_id|
        @backend.ack_on(group_id, message_id)
      end
    end

    # Register consumer groups for all reactors in this unit.
    # Must run inside the transaction before processing begins.
    def ensure_consumer_groups!
      @reactors.each do |reactor|
        @backend.register_consumer_group(reactor.consumer_info.group_id)
      end
    end

    # Build a lookup table mapping message classes to the reactor classes
    # that handle them.
    #
    # @return [Hash{Class => Array<Class>}]
    def build_routing_table
      table = {}
      @reactors.each do |reactor|
        reactor.handled_messages.each do |msg_class|
          table[msg_class] ||= []
          table[msg_class] << reactor
        end
      end
      table
    end

    # Pre-resolve keyword argument names for each reactor's #handle method.
    #
    # @return [Hash{Class => Array<Symbol>}]
    def resolve_kargs
      @reactors.each_with_object({}) do |reactor, hash|
        hash[reactor] = Injector.resolve_args(reactor, :handle)
      end
    end

    # Collects reactor instances, produced events, and ACK pairs
    # during a {Unit#handle} run.
    #
    # @example Inspecting results after handle
    #   results = unit.handle(cmd)
    #
    #   # Get a hash of { instance => [events] } for a reactor class
    #   results[MyActor].each do |instance, events|
    #     puts "#{instance.id}: #{events.map(&:class)}"
    #   end
    #
    #   # Get a flat list of events for a reactor class
    #   results.events_for(MyActor) # => [MyEvent, ...]
    class Results
      def initialize
        @data = {}
        @acks = []
      end

      # Record a reactor invocation and the events it produced.
      #
      # @param reactor_class [Class]
      # @param instance [Object] the reactor instance
      # @param produced_events [Array<Sourced::Message>]
      # @param handled_message [Sourced::Message] the message that was handled (for ACK tracking)
      def record(reactor_class, instance, produced_events, handled_message)
        key = [reactor_class, instance.id]
        entry = (@data[key] ||= { instance: nil, events: [] })
        entry[:instance] = instance
        entry[:events].concat(produced_events)

        @acks << [reactor_class.consumer_info.group_id, handled_message.id]
      end

      # Get a hash of reactor instances and their produced events.
      #
      # @param reactor_class [Class]
      # @return [Hash{Object => Array<Sourced::Message>}] instance => events
      #
      # @example
      #   results[MyActor].each do |instance, events|
      #     puts instance.state
      #   end
      def [](reactor_class)
        result = {}
        @data.each do |(klass, _stream_id), entry|
          next unless klass == reactor_class

          result[entry[:instance]] = entry[:events]
        end
        result
      end

      # Get a flat list of all events produced by a reactor class.
      #
      # @param reactor_class [Class]
      # @return [Array<Sourced::Message>]
      #
      # @example
      #   results.events_for(MyActor) # => [OrderPlaced, PaymentCharged]
      def events_for(reactor_class)
        self[reactor_class].values.flatten
      end

      # Iterate over all ACK pairs (group_id, message_id).
      #
      # @yield [group_id, message_id]
      # @yieldparam group_id [String]
      # @yieldparam message_id [String]
      def each_ack(&block)
        @acks.each do |group_id, message_id|
          block.call(group_id, message_id)
        end
      end
    end
  end
end
