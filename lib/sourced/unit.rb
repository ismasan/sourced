# frozen_string_literal: true

require 'sourced/injector'

module Sourced
  class Unit
    DEFAULT_MAX_ITERATIONS = 100
    InfiniteLoopError = Class.new(Sourced::Error)

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

    def should_persist?(message)
      return true if @persist_commands

      message.is_a?(Sourced::Event)
    end

    def handle_for_reactor(reactor_class, message, results)
      instance = reactor_class.new(id: reactor_class.identity_from(message))
      history = @backend.read_stream(message.stream_id)

      kargs = build_handle_args(reactor_class, history)
      actions = instance.handle(message, **kargs)

      new_messages, produced_events = process_actions(message, actions)

      results.record(reactor_class, instance, produced_events, message)

      new_messages
    end

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

    def handled_by_unit?(message)
      @routing_table.key?(message.class)
    end

    def ack_all(results)
      results.each_ack do |group_id, message_id|
        @backend.ack_on(group_id, message_id)
      end
    end

    def ensure_consumer_groups!
      @reactors.each do |reactor|
        @backend.register_consumer_group(reactor.consumer_info.group_id)
      end
    end

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

    def resolve_kargs
      @reactors.each_with_object({}) do |reactor, hash|
        hash[reactor] = Injector.resolve_args(reactor, :handle)
      end
    end

    class Results
      def initialize
        @data = {}
        @acks = []
      end

      def record(reactor_class, instance, produced_events, handled_message)
        key = [reactor_class, instance.id]
        entry = (@data[key] ||= { instance: nil, events: [] })
        entry[:instance] = instance
        entry[:events].concat(produced_events)

        @acks << [reactor_class.consumer_info.group_id, handled_message.id]
      end

      def [](reactor_class)
        result = {}
        @data.each do |(klass, _stream_id), entry|
          next unless klass == reactor_class

          result[entry[:instance]] = entry[:events]
        end
        result
      end

      def events_for(reactor_class)
        self[reactor_class].values.flatten
      end

      def each_ack(&block)
        @acks.each do |group_id, message_id|
          block.call(group_id, message_id)
        end
      end
    end
  end
end
