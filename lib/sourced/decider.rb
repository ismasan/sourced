# frozen_string_literal: true

module Sourced
  # A Decider that decides one command: a self-contained decision, reading
  # +source+ (a {Selector} or an inline projector, see {SourceInterface}) and
  # emitting +outputs+. Its consumer group is the command's type, and its
  # partition keys the source selector's, unless chained:
  #
  #   AddItem = Sourced::Decider(Selector[CartStarted, ItemAdded].by(:cart_id), Add => ItemAdded) do |history, cmd|
  #     event ItemAdded, **cmd.payload.to_h
  #   end
  #
  #   Reserve = Sourced::Decider(StockProjector, ReserveStock => StockReserved | Rejected) do |stock, cmd|
  #     # ...
  #   end.partition_by(:sku)
  #
  # Without a source, the block takes the command only, and without partition
  # keys it's an id-partitioned queue (see {Consumer#exclusive}).
  #
  # @param source [SourceInterface, nil]
  # @param declaration [Hash] one +Command => outputs+ pair (see {Decider.command})
  # @yield [context, command], or +[command]+
  # @return [Class<Decider>]
  def self.Decider(source = nil, declaration = nil, &block)
    return Decider.__single(nil, source, &block) if declaration.nil?

    Decider.__single(source, declaration, &block)
  end

  # Reactor base class for command-handling workflows in Sourced.
  class Decider
    include Sourced::Evolve
    include Sourced::React
    include Sourced::Sync
    extend Sourced::Consumer

    PREFIX = 'sourced_decide'

    # Raised when a command handler emits an event carrying another
    # partition's value for one of the partition keys.
    InvalidEventError = Class.new(Error)

    # Raised when a command handler emits a message its command doesn't
    # declare, or emits nothing when its outputs don't include +nil+.
    UndeclaredEventError = Class.new(UndeclaredOutputError)

    class << self
      # @return [Array<Class>] command message classes handled by this decider
      def handled_commands
        @handled_commands ||= []
      end

      # Messages to claim: commands to decide on + events to react to.
      # Evolve types are NOT included — they are only for the history selector.
      #
      # @return [Array<Class>] command and reaction message classes
      def handled_messages
        handled_commands + handled_messages_for_react
      end

      # @return [Hash{Class => Outputs}] each handled command's declared outputs
      def command_outputs
        @command_outputs ||= {}
      end

      # @return [Array<Class>] every message class a command handler may emit
      def produced_messages = command_outputs.values.flat_map(&:messages).uniq

      # Register a command handler, and what it may emit: a message class, a
      # union of them, with +| nil+ when it may emit none.
      #
      # @example
      #   command AddItem => ItemAdded | ItemRejected do |context, cmd|
      #     event ItemAdded, cart_id: cmd.payload.cart_id, sku: cmd.payload.sku
      #   end
      #
      # @param declaration [Hash{Class => Class, Plumb::Or, nil}] one
      #   +Command => outputs+ pair (commands and outputs as {Sourced::Message.to_message_class})
      # @yield [context, message] command handler block; a block taking one
      #   argument gets the message only
      # @return [void]
      # @raise [ArgumentError] without exactly one pair, or for outputs that
      #   aren't message classes
      def command(declaration, &block)
        unless declaration.is_a?(Hash) && declaration.size == 1
          raise ArgumentError, "#{self}.command takes one Command => outputs pair, " \
                               "ex. command #{declaration.inspect} => SomeEvent, got #{declaration.inspect}"
        end

        declared, outputs = declaration.first
        message_class = declared.to_message_class
        begin
          command_outputs[message_class] = Outputs.parse(outputs)
        rescue ArgumentError => e
          raise ArgumentError, "#{self}.command #{message_class}: #{e.message}"
        end
        handled_commands << message_class
        handler = block.arity == 1 ? proc { |_context, cmd| instance_exec(cmd, &block) } : block
        define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &handler)
      end

      # @api private
      # @see Sourced.Decider
      def __single(source, declaration, &block)
        unless declaration.is_a?(Hash) && declaration.size == 1
          raise ArgumentError, "Sourced::Decider takes one Command => outputs pair, got #{declaration.inspect}"
        end

        input = declaration.keys.first.to_message_class
        Class.new(self) do
          consumer_group input.type
          if source
            evolve source
            keys = source.selector.keys
            partition_by(*keys) if keys.any?
          end
          command(declaration, &block)
          define_singleton_method(:exclusive?) { partition_keys.empty? || super() }
        end
      end

      # Decide +command+ from +history+, without a store: what {Sourced.handle!}
      # does, minus reading and appending. The partition is the command's
      # values for the partition keys (payload, else tags).
      #
      # @example
      #   Carts.call(history, AddItem.new(payload: { cart_id: 'c1', sku: 'apple' })) # => [ItemAdded]
      #
      # @param history [#to_a] the partition's history before the command
      #   (messages, or a {ReadResult})
      # @param command [Sourced::Message]
      # @return [Array<Sourced::Message>] the events it decides
      # @raise [Sourced::MissingPartitionKeyError] when the command has no value for a partition key
      # @raise [Sourced::CommandNotHandledError] when it doesn't handle the command (see {#handles_command?})
      def call(history, command)
        instance = new(__partition_values_of(command))
        instance.evolve(history.to_a)
        unless instance.handles_command?(command)
          raise CommandNotHandledError, "#{self} doesn't handle #{command.class} (#{command.type}) in its current state"
        end

        instance.decide(command)
      end

      # Build executable actions for a batch of claimed messages.
      #
      # Each message in +new_messages+ routes through one of three branches:
      #
      # 1. **Command** — if the message class is in {.handled_commands}, the
      #    decider runs {Decider#decide}, produces new events, and wraps them
      #    in an {Actions::Append} (guarded by +history.guard+). Per-message
      #    +sync+ / +after_sync+ actions are collected. A command the decider
      #    doesn't handle in its current context ({Decider#handles_command?}) is
      #    acked with no events.
      #
      # 2. **Reaction-triggering event** — if the decider
      #    {React#reacts_to? reacts to} this message and
      #    {React#should_react?} allows it (by default: the batch is not
      #    replaying), its context is evolved with the event, {React#react} is
      #    invoked, and any produced messages are wrapped in an
      #    {Actions::Append} with +source: msg+ so the infra layer correlates
      #    reaction messages against the event (future-dated ones are deferred
      #    to the scheduled_messages table by {Sourced::Store#append}).
      #
      # 3. **Anything else** — yields +[Actions::OK, msg]+ (the message is
      #    acked with no side effects). In practice this is unreachable
      #    because claim filtering uses {.handled_messages}, but it's kept
      #    as a safety net.
      #
      # ### Reactions are deferred
      #
      # Reactions do **not** run inline with the command that produced the
      # triggering event. A command's batch only appends its events; the
      # decider's own subscription (via +handled_messages_for_react+) picks
      # those events up on the next claim cycle and runs the reaction in a
      # separate +handle_batch+ invocation.
      #
      # Consequences:
      #
      # - The originating command's +after_sync+ fires as soon as its
      #   events commit — no longer blocked by slow reactions.
      # - Command and its reactions are in **separate transactions**. A
      #   failing reaction does not roll back the command.
      # - There is one extra claim-cycle of latency before reactions run
      #   (sub-ms via +InlineNotifier+; up to +catchup_interval+ as
      #   fallback).
      # - +sync+ / +after_sync+ blocks also fire on reaction claims
      #   (+messages: [evt]+, +events: []+) — inspectors that assume the
      #   primary message is always a command will see different shapes.
      #
      # @param partition_values [Hash{Symbol => String}] partition key-value pairs
      # @param new_messages [Array<PositionedMessage>] claimed messages to process
      # @param history [ReadResult] prior partition history (evolved into context)
      # @param replaying [Boolean] forwarded to {React#should_react?}, which by
      #   default skips the reaction branch when +true+
      # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
      def handle_batch(partition_values, new_messages, history:, replaying: false)
        instance = new(partition_values)
        instance.evolve(history.messages)

        each_with_partial_ack(new_messages) do |msg|
          if handled_commands.include?(msg.to_message.class)
            next [Actions::OK, msg] unless instance.handles_command?(msg)

            raw_events = instance.decide(msg)
            actions = Actions.build_for(raw_events, guard: history.guard, source: msg)
            # The runner correlates the events as it appends them; +events:+
            # is bound then, so the hooks see the events as stored.
            actions += instance.collect_actions(context: instance.context, messages: [msg]) do |appended|
              { events: appended }
            end

            [actions, msg]
          elsif instance.reacts_to?(msg) && instance.should_react?(instance.context, msg, replaying:)
            instance.evolve([msg])
            reaction_msgs = Array(instance.react(msg))
            actions = Actions.build_for(reaction_msgs, source: msg)
            actions += instance.collect_actions(
              context: instance.context, messages: [msg], events: []
            )

            [actions, msg]
          else
            [Actions::OK, msg]
          end
        end
      end

      # Build executable actions for a claimed batch.
      #
      # +history+ comes from an unbounded read, so it contains the claimed
      # messages; it is narrowed to prior history before being handed to
      # {.handle_batch}, which evolves the claimed batch itself.
      #
      # @param claim [ClaimResult] claimed partition batch
      # @param history [ReadResult] event history for the partition
      # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
      def handle_claim(claim, history:)
        values = partition_keys.to_h { |k| [k, claim.partition_value[k.to_s]] }
        handle_batch(values, claim.messages, history: history.excluding(claim.messages), replaying: claim.replaying)
      end

      private def __partition_values_of(command)
        values = Sourced.extract_partition_attrs(command.to_message, partition_keys).transform_values(&:to_s)
        missing = partition_keys - values.keys
        return values if missing.empty?

        raise MissingPartitionKeyError, "#{command.type} has no value for #{self}'s partition keys #{missing.inspect}"
      end

      # Copy registered command handlers into subclasses.
      #
      # @param subclass [Class] subclass being created
      # @return [void]
      def inherited(subclass)
        super
        handled_commands.each do |cmd_class|
          subclass.handled_commands << cmd_class
        end
        subclass.command_outputs.merge!(command_outputs)
      end
    end

    attr_reader :partition_values

    # @param partition_values [Hash{Symbol => String}] partition key-value pairs
    def initialize(partition_values = {})
      @partition_values = partition_values
      @uncommitted_events = []
    end

    # Whether this decider handles +command+ in its current context.
    #
    # {Sourced.handle!} raises {Sourced::CommandNotHandledError} when this is
    # false, without appending anything; in background workers, {.handle_batch}
    # acks the command with no events. Defaults to whether a handler is
    # registered for the command's class, and its precondition holds: its
    # history selector's assertion (a selector source's), over the context so
    # far, bound to the partition. An inline projector source has none.
    #
    # Override to refuse commands based on context without raising, the command-side
    # counterpart of {React#should_react?}:
    #
    #   def handles_command?(command)
    #     super && !context[:archived]
    #   end
    #
    # Raising in a +command+ handler still rejects the command loudly: through the
    # error strategy in workers, as an exception in {Sourced.handle!}.
    #
    # @param command [Sourced::Message]
    # @return [Boolean]
    def handles_command?(command)
      self.class.handled_commands.include?(command.to_message.class) && precondition_holds?
    end

    # Decide a command against the decider's current in-memory context.
    #
    # @param command [Sourced::Message] command to handle
    # @return [Array<Sourced::Message>] newly produced events
    # @raise [UndeclaredEventError] when the handler emits nothing and its
    #   outputs don't include +nil+
    def decide(command)
      @uncommitted_events = []
      klass = command.to_message.class
      @deciding = self.class.command_outputs[klass]
      method_name = Sourced.message_method_name(PREFIX, klass.to_s)
      send(method_name, context, command) if respond_to?(method_name)
      if @deciding && @uncommitted_events.empty? && !@deciding.optional
        raise UndeclaredEventError, "#{self.class}: the #{klass} handler emitted nothing; " \
                                    "declare command #{klass} => #{@deciding} | nil to allow it"
      end

      @uncommitted_events.dup
    ensure
      @deciding = nil
    end

    # Produce a new event from within a command handler and apply it
    # to the decider's in-memory context immediately.
    #
    # @param event_class [Class] event class
    # @param payload [Hash] payload attributes for the event
    # @return [Sourced::Message] the newly built event
    #
    # @example
    #   command RegisterDevice => DeviceRegistered do |_context, cmd|
    #     event DeviceRegistered, device_id: cmd.payload.device_id
    #   end
    # @raise [UndeclaredEventError] when the command being decided doesn't
    #   declare +event_class+
    # @raise [InvalidEventError] when the event carries another partition's
    #   value for a partition key
    def event(event_class, payload = {})
      if @deciding && !@deciding.include?(event_class.to_message_class)
        raise UndeclaredEventError, "#{self.class}: emitted #{event_class}, which its command doesn't declare " \
                                    "(it declares #{@deciding})"
      end

      evt = event_class.new(payload: payload)
      check_partition!(evt)
      @uncommitted_events << evt
      evolve([evt])
      evt
    end

    private

    def precondition_holds?
      return true if self.class.evolve_selector.assertion == Selector::Assertion::TRUE

      self.class.history_selector.bind(**partition_values).holds?(context)
    end

    # What the decider emits belongs to its partition: an event carrying a
    # partition key with another value would be read into another partition.
    def check_partition!(evt)
      Sourced.extract_partition_attrs(evt, self.class.partition_keys).each do |key, value|
        value = value.to_s
        expected = partition_values[key]
        next if expected.nil? || value == expected.to_s

        raise InvalidEventError, "#{self.class}: #{evt.class} has #{key} #{value.inspect}, but is emitted in " \
                                 "the partition where #{key} is #{expected.to_s.inspect}"
      end
    end
  end
end
