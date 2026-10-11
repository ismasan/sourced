# frozen_string_literal: true

module Sourced
  # Reactor base class for command-handling workflows in Sourced.
  class Decider
    include Sourced::Evolve
    include Sourced::React
    include Sourced::Sync
    extend Sourced::Consumer

    PREFIX = 'sourced_decide'

    class << self
      # @return [Array<Class>] command message classes handled by this decider
      def handled_commands
        @handled_commands ||= []
      end

      # Messages to claim: commands to decide on + events to react to.
      # Evolve types are NOT included — they are only for context_for().
      #
      # @return [Array<Class>] command and reaction message classes
      def handled_messages
        handled_commands + handled_messages_for_react
      end

      # Register a command handler.
      #
      # @param message_class [Class] commands class to handle
      # @yield [state, message] command handler block
      # @return [void]
      def command(message_class, &block)
        handled_commands << message_class
        define_method(Sourced.message_method_name(PREFIX, message_class.to_s), &block)
      end

      # Build executable actions for a batch of claimed messages.
      #
      # Each message in +new_messages+ routes through one of three branches:
      #
      # 1. **Command** — if the message class is in {.handled_commands}, the
      #    decider runs {Decider#decide}, produces new events, and wraps them
      #    in an {Actions::Append} (guarded by +history.guard+). Per-message
      #    +sync+ / +after_sync+ actions are collected. A command the decider
      #    doesn't handle in its current state ({Decider#handles_command?}) is
      #    acked with no events.
      #
      # 2. **Reaction-triggering event** — if the decider
      #    {React#reacts_to? reacts to} this message and
      #    {React#should_react?} allows it (by default: the batch is not
      #    replaying), its state is evolved with the event, {React#react} is
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
      # @param history [ReadResult] prior partition history (evolved into state)
      # @param replaying [Boolean] forwarded to {React#should_react?}, which by
      #   default skips the reaction branch when +true+
      # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
      def handle_batch(partition_values, new_messages, history:, replaying: false)
        instance = new(partition_values)
        instance.evolve(history.messages)

        each_with_partial_ack(new_messages) do |msg|
          if handled_commands.include?(msg.class)
            next [Actions::OK, msg] unless instance.handles_command?(msg)

            raw_events = instance.decide(msg)
            actions = Actions.build_for(raw_events, guard: history.guard, source: msg)
            # The runner correlates the events as it appends them; +events:+
            # is bound then, so the hooks see the events as stored.
            actions += instance.collect_actions(state: instance.state, messages: [msg]) do |appended|
              { events: appended }
            end

            [actions, msg]
          elsif instance.reacts_to?(msg) && instance.should_react?(instance.state, msg, replaying:)
            instance.evolve([msg])
            reaction_msgs = Array(instance.react(msg))
            actions = Actions.build_for(reaction_msgs, source: msg)
            actions += instance.collect_actions(
              state: instance.state, messages: [msg], events: []
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

      # Copy registered command handlers into subclasses.
      #
      # @param subclass [Class] subclass being created
      # @return [void]
      def inherited(subclass)
        super
        handled_commands.each do |cmd_class|
          subclass.handled_commands << cmd_class
        end
      end
    end

    attr_reader :partition_values

    # @param partition_values [Hash{Symbol => String}] partition key-value pairs
    def initialize(partition_values = {})
      @partition_values = partition_values
      @uncommitted_events = []
    end

    # Whether this decider handles +command+ in its current state.
    #
    # {Sourced.handle!} raises {Sourced::CommandNotHandledError} when this is
    # false, without appending anything; in background workers, {.handle_batch}
    # acks the command with no events. Defaults to whether a handler is
    # registered for the command's class.
    #
    # Override to refuse commands based on state without raising, the command-side
    # counterpart of {React#should_react?}:
    #
    #   def handles_command?(command)
    #     super && !state[:archived]
    #   end
    #
    # Raising in a +command+ handler still rejects the command loudly: through the
    # error strategy in workers, as an exception in {Sourced.handle!}.
    #
    # @param command [Sourced::Message]
    # @return [Boolean]
    def handles_command?(command)
      self.class.handled_commands.include?(command.class)
    end

    # Decide a command against the decider's current in-memory state.
    #
    # @param command [Sourced::Message] command to handle
    # @return [Array<Sourced::Message>] newly produced events
    def decide(command)
      @uncommitted_events = []
      method_name = Sourced.message_method_name(PREFIX, command.class.to_s)
      send(method_name, state, command) if respond_to?(method_name)
      @uncommitted_events.dup
    end

    # Produce a new event from within a command handler and apply it
    # to the decider's in-memory state immediately.
    #
    # @param event_class [Class] event class
    # @param payload [Hash] payload attributes for the event
    # @return [Sourced::Message] the newly built event
    #
    # @example
    #   command RegisterDevice do |_state, cmd|
    #     event DeviceRegistered, device_id: cmd.payload.device_id
    #   end
    def event(event_class, payload = {})
      evt = event_class.new(payload: payload)
      @uncommitted_events << evt
      evolve([evt])
      evt
    end
  end
end
