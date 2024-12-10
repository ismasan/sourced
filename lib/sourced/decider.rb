# frozen_string_literal: true

module Sourced
  # A "Decider" is an object that encapsulates the process of handling a command and updating system state via events.
  # This is the "controller" in a Sourced app.
  # All capabilities in an app are commands handled by Deciders.
  # The top-level API is:
  #   cmd = SomeCommand.new(stream_id: '123', payload: { ... })
  #   state, new_events = SomeDecider.handle_command(cmd)
  # 
  # This will:
  # 1. Initialize in-memory state with a stream_id.
  # 2. Load event history from backend for that stream_id.
  # 3. Project the events onto the state, to get at the current state.
  # 4. Yield the state and command to the command handler.
  # 5. Capture any events returned by the command handler.
  # 6. Persist the new events to the backend.
  # 7. Run any synchronous blocks registered with the Decider in the same transaction.
  # 8. Return the updated state and new events.
  #
  # command => Decider[init state, load events, evolve state, handle command, apply events, commit, sync blocks] => [new state, new events]
  #
  # Sourced::Decider defines a DSL to define command handlers, event evolvers and "reactions".
  # See https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/
  # But Sourced will accept anything that implements the top level Decider API:
  #
  #    .handle_command(Sourced::Command) => [state Any, new_events Array<Sourced::Event>]
  #    .handled_commands() => Array<Sourced::Command>
  #
  # You can also use the #decide(command) => Aray<Events> method directly.
  # for testing behaviour via commands and events without touching the DB.
  #
  #    decider = SomeDecider.new('123')
  #    state, new_events = decider.decide(cmd)
  #
  # Full definition example:
  #
  #    module Leads
  #      # Define commands and events
  #      CreateLead = Sourced::Command.define('leads.create') do
  #        attribute :name, Plumb::Types::String.present
  #        attribute :email, Plumb::Types::Email.present
  #      end
  #
  #      LeadCreated = Sourced::Event.define('leads.created') do
  #        attribute :name, Plumb::Types::String.present
  #        attribute :email, Plumb::Types::Email.present
  #      end
  #
  #      # Leads::LeadDecider encapsulates working with Leads
  #      class LeadDecider < Sourced::Decider
  #        # First define what initial state looks like
  #        # This can return any object that makes sense for the app.
  #        # This state will be updated by applying events on it,
  #        # and its purpose is to support any questions the app needs to answer
  #        # before deciding to emit new events.
  #        def init_state(stream_id)
  #          { 
  #            id: stream_id, 
  #            name: nil, 
  #            email: nil, 
  #            status: 'new',
  #            email_sent: false
  #          }
  #        end
  #
  #        # ====== DECIDE BLOCK =================
  #        # Define a command handler for the Leads::CreateLead command
  #        # The command handler takes the current state of the lead
  #        # (which will be the initial state Hash when first creating the lead)
  #        # and the command instance coming from the client or UI or another workflow.
  #        decide CreateLead do |state, cmd|
  #          # Apply a LeadCreated event to the state
  #          # events applied here will be stored in the backend
  #          apply LeadCreated, name: cmd.payload.name, email: cmd.payload.email
  #
  #          # If the command and event share the same payload attributes, you can just do:
  #          apply LeadCreated, cmd.payload
  #        end
  #
  #        # ====== EVOLVE BLOCK =================
  #        # .evolve blocks define how an event updates the state
  #        # These blocks are run before running a .decide block
  #        # To update the state object from past events
  #        # They're also run within a .decide block when applying new events with `#apply`
  #        evolve LeadCreated do |state, event|
  #          state[:status] = 'created'
  #          state[:name] = event.payload.name
  #          state[:email] = event.payload.email
  #        end
  #
  #        # ====== REACT BLOCK =================
  #        # React blocks listen to events emitted by .decide blocks
  #        # From this or any other Decider
  #        # and allow an part of the app to react to events
  #        # This is how you build worlflows that span multiple Deciders
  #        # React blocks run in the background, and therefore these workflows
  #        # are eventually consistent, unless configured to be synchronous.
  #        # In this example we listen to LeadCreated events
  #        # and schedule a new SendEmail command
  #        react LeadCreated do |event|
  #          event.follow(SendEmail)
  #        end
  #
  #        # We now handle the SendEmail command and the cycle starts again.
  #        decide SendEmail do |state, cmd|
  #          raise 'Email already sent' if state[:email_sent]
  #
  #          if Mailer.deliver_to(state[:email], state)
  #            apply EmailSent
  #          else
  #            apply EmailFailed
  #          end
  #        end
  #
  #        evolve EmailSent do |state, event|
  #          state[:email_sent] = true
  #        end
  #      end
  #    end
  class Decider
    include Evolve
    include React
    include Sync
    extend Consumer

    PREFIX = 'decide'

    class << self
      def inherited(subclass)
        super
        handled_commands.each do |cmd_type|
          subclass.handled_commands << cmd_type
        end
      end

      # Register as a Reactor
      # @return [Array<Sourced::Event>]
      def handled_events = self.handled_events_for_react

      # The Reactor interface
      # Any commands returned will be schedule to run next.
      #
      # @param events [Array<Sourced::Event>]
      # @return [Array<Sourced::Command>]
      def handle_events(events)
        load(events.first.stream_id).handle_events(events)
      end

      # The Decider interface
      # Initialize and load state from past event,
      # run the command handler and return the new state and events
      #
      # @param cmd [Sourced::Command]
      # @return [Array<Any, Array<Sourced::Event>]
      def handle_command(cmd)
        load(cmd.stream_id).handle_command(cmd)
      end

      # Load a Decider from event history
      #
      # @param stream_id [String] the stream id
      # @return [Decider]
      def load(stream_id, upto: nil)
        new(stream_id).load(upto:)
      end

      def handled_commands
        @handled_commands ||= []
      end

      # The .decide macro.
      # Register a command handler
      # @example
      #
      #  decide SomeCommand do |state, cmd|
      #    apply SomeEvent, field1: cmd.payload.field1, field2: 'foobar'
      #  end
      def decide(cmd_type, &block)
        handled_commands << cmd_type
        define_method(Sourced.message_method_name(PREFIX, cmd_type.name), &block)
      end

      # Define a command class, register a command handler
      # and define a method to send the command
      # Example:
      #   command :add_item, name: String do |cmd|
      #     cmd.follow(ItemAdded, item_id: SecureRandom.uuid, name: cmd.payload.name)
      #   end
      #
      # # The exmaple above will define a command class `AddItem` in the current namespace:
      # AddItem = Message.define('namespace.add_item', payload_schema: { name: String })
      #
      # # Optionally you can pass an explicit command type string:
      #   command :add_item, 'todos.items.add', name: String do |cmd|
      #
      # # It will also register the command handler:
      # decide AddItem do |cmd|
      #   cmd.follow(ItemAdded, item_id: SecureRandom.uuid, name: cmd.payload.name)
      # end
      #
      # # And an :add_item method to send the command:
      # def add_item(name:)
      #   issue_command AddItem, name:
      # end
      #
      # This method can be used on Decider instances:
      #   aggregate.add_item(name: 'Buy milk')
      #
      # Payload schema is a Plumb Hash schema.
      # See: https://github.com/ismasan/plumb#typeshash
      #
      # The helper method will instantiate an instance of the command class
      # and validate its attributes with #valid?
      # Only valid commands will be issued to the handler.
      # The method returns the command instance. If #valid? is false, then the command was not issued.
      # Example:
      #   cmd = aggregate.add_item(name: 10)
      #   cmd.valid? # => false
      #   cmd.errors # => { name: 'must be a String' }
      #
      # @param cmd_name [Symbol] example: :add_item
      # @param payload_schema [Hash] A Plumb Hash schema. example: { name: String }
      # @param block [Proc] The command handling code
      # @return [Message] the command instance, which can be #valid? or not
      def command(*args, &block)
        raise ArgumentError, 'command block expects signature (state, command)' unless block.arity == 2

        message_type = nil
        cmd_name = nil
        payload_schema = {}
        segments = name.split('::').map(&:downcase)

        case args
          in [cmd_name]
            message_type = [*segments, cmd_name].join('.')
          in [cmd_name, Hash => payload_schema]
            message_type = [*segments, cmd_name].join('.')
          in [cmd_name, String => message_type, Hash => payload_schema]
          in [cmd_name, String => message_type]
        else
          raise ArgumentError, "Invalid arguments for #{self}.command"
        end

        klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
        cmd_class = Command.define(message_type, payload_schema:)
        const_set(klass_name, cmd_class)
        decide cmd_class, &block
        define_method(cmd_name) do |**payload|
          issue_command cmd_class, payload
        end
      end
    end

    attr_reader :id, :seq, :state, :uncommitted_events

    def initialize(id = SecureRandom.uuid, backend: Sourced.config.backend, logger: Sourced.config.logger)
      @id = id
      @backend = backend
      @logger = logger
      @seq = 0
      @state = init_state(id)
      @uncommitted_events = []
      @__current_command = nil
    end

    def inspect
      %(<#{self.class} id:#{id} seq:#{seq}>)
    end

    def ==(other)
      other.is_a?(self.class) && id == other.id && seq == other.seq
    end

    # Initialize in-memory state for this decider.
    # Override this method to provide a custom initial state.
    # @param id [String] the stream id
    # @return [Any] the initial state
    def init_state(id)
      nil
    end

    # Load event history from backend
    # and evolve the state
    #
    # @option after [Integer] the sequence number to start loading from
    # @option upto [Integer] the sequence number to load up to
    def load(after: nil, upto: nil)
      events = backend.read_event_stream(id, after:, upto:)
      if events.any?
        @seq = events.last.seq 
        evolve(state, events)
      end
      self
    end

    # Load event history from backend from the last event sequence number we know about
    # Use this to update local state when events for this decider
    # may have been emitted by other deciders or processes in the background
    # Returns the old and new sequence numbers
    # @return [Array<Integer, Integer>]
    def catch_up
      seq_was = seq
      load(after: seq_was)
      [seq_was, seq]
    end

    # Load and return events for this decider
    # @option upto [Integer] the sequence number to load up to
    # @option after [Integer] the sequence number to start loading from
    # @return [Array<Sourced::Event>]
    def events(upto: seq)
      backend.read_event_stream(id, upto:)
    end

    # Route a command to its defined command handler, and run it.
    # @param command [Sourced::Command]
    # @return [Array<Any, Array<Sourced::Event>]
    def decide(command)
      command = __set_current_command(command)
      send(Sourced.message_method_name(PREFIX, command.class.name), state, command)
      @__current_command = nil
      [state, uncommitted_events]
    end

    # Instantiate an event class and apply it to the state
    # by running registered evolve blocks.
    # Also store the event in the uncommitted events list,
    # and keep track of the sequence number.
    # To be used inside a .decide block.
    # @example
    #
    #  apply SomeEvent, field1: 'foo', field2: 'bar'
    #
    # @param event_class [Sourced::Event]
    # @param payload [Hash] the event payload
    # @return [Any] the new state
    def apply(event_class, payload = {})
      evt = __current_command.follow_with_attributes(
        event_class, 
        attrs: { seq: __next_sequence }, 
        metadata: { producer: self.class.consumer_info.group_id },
        payload:
      )
      uncommitted_events << evt
      evolve(state, [evt])
    end

    # Commit uncommitted events to the backend
    # This will run in a transaction
    # If the transaction succeeds, the uncommitted events will be cleared
    # If the transaction raises an exception, the uncommitted events will remain
    # so that they can be retried.
    def commit(&)
      output_events = uncommitted_events.slice(0..-1)
      yield output_events
      @uncommitted_events = []
      output_events
    end

    # Register a first sync block to append new events to backend
    sync do |_state, command, events|
      messages = [command, *events]
      backend.append_to_stream(id, messages) if messages.any?
    end

    # Register a second sync block to route events to any
    # SYNC reactors that are listening to events emitted by this Decider.
    sync do |_state, command, events|
      Sourced::Router.handle_events(events)
    end

    # Take a command instance
    # Route it to its registered command handler
    # and save the resulting events to the backend
    # also running any sync blocks.
    # @param command [Sourced::Command]
    # @return [Array<Any, Array<Sourced::Event>]
    def handle_command(command)
      # TODO: this might raise an exception from a worker
      # Think what to do with invalid commands here
      raise "invalid command #{command.inspect} #{command.errors.inspect}" unless command.valid?
      logger.info "#{self.inspect} Handling #{command.type}"
      decide(command)
      save
    end

    # Reactor interface
    # Handle events, return new commands
    # Workers will handle routing these commands
    # to their target Deciders
    # @param events [Array<Sourced::Event>]
    # @return [Array<Sourced::Command>]
    def handle_events(events)
      react(events)
    end

    private

    attr_reader :backend, :logger, :__current_command

    def save
      events = commit do |messages|
        backend.transaction do
          run_sync_blocks(state, messages[0], messages[1..-1])
        end
      end
      [ self, events ]
    end

    def __set_current_command(command)
      command.with(seq: __next_sequence).tap do |cmd|
        uncommitted_events << cmd
        @__current_command = cmd
      end
    end

    def __next_sequence
      @seq += 1
    end

    def issue_command(klass, payload = {})
      cmd = klass.new(stream_id: id, payload:)
      return cmd unless cmd.valid?

      handle_command(cmd)
      cmd
    end
  end
end
