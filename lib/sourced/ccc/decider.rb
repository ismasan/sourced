# frozen_string_literal: true

module Sourced
  module CCC
    # Reactor base class for command-handling workflows in CCC.
    class Decider
      include CCC::Evolve
      include CCC::React
      include CCC::Sync
      extend CCC::Consumer

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
        # @param message_class [Class] CCC command class to handle
        # @yield [state, message] command handler block
        # @return [void]
        def command(message_class, &block)
          handled_commands << message_class
          define_method(Sourced.message_method_name('ccc_decide', message_class.to_s), &block)
        end

        # Build executable actions for a claimed batch.
        #
        # @param claim [ClaimResult] claimed partition batch
        # @param history [ReadResult] event history for the partition
        # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
        def handle_batch(claim, history:)
          values = partition_keys.map { |k| claim.partition_value[k.to_s] }
          instance = new(values)
          instance.evolve(history.messages)

          each_with_partial_ack(claim.messages) do |msg|
            if handled_commands.include?(msg.class)
              raw_events = instance.decide(msg)
              correlated_events = raw_events.map { |e| msg.correlate(e) }
              actions = []
              actions.concat(
                Actions.build_for(correlated_events, guard: history.guard, correlated: true)
              )

              correlated_events.each do |evt|
                next unless instance.reacts_to?(evt)
                reaction_msgs = Array(instance.react(evt))
                actions.concat(Actions.build_for(reaction_msgs, source: evt))
              end

              actions += instance.sync_actions(
                state: instance.state, messages: [msg], events: raw_events
              )

              [actions, msg]
            else
              [Actions::OK, msg]
            end
          end
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

      # @param partition_values [Array<String, nil>] values for the decider's partition keys
      def initialize(partition_values = [])
        @partition_values = partition_values
        @uncommitted_events = []
      end

      # Decide a command against the decider's current in-memory state.
      #
      # @param command [CCC::Message] command to handle
      # @return [Array<CCC::Message>] newly produced events
      def decide(command)
        @uncommitted_events = []
        method_name = Sourced.message_method_name('ccc_decide', command.class.to_s)
        send(method_name, state, command) if respond_to?(method_name)
        @uncommitted_events.dup
      end

      # Produce a new event from within a command handler and apply it
      # to the decider's in-memory state immediately.
      #
      # Accepts either a CCC message class or a symbol resolved via <tt>.[]</tt>.
      #
      # @param event_class [Class, Symbol] event class or symbolic event name
      # @param payload [Hash] payload attributes for the event
      # @return [CCC::Message] the newly built event
      #
      # @example Produce by class
      #   command RegisterDevice do |_state, cmd|
      #     event DeviceRegistered, device_id: cmd.payload.device_id
      #   end
      #
      # @example Produce by symbol
      #   command RegisterDevice do |_state, cmd|
      #     event :device_registered, device_id: cmd.payload.device_id
      #   end
      def event(event_class, payload = {})
        event_class = self.class[event_class] if event_class.is_a?(Symbol)
        evt = event_class.new(payload: payload)
        @uncommitted_events << evt
        evolve([evt])
        evt
      end
    end
  end
end
