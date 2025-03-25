# frozen_string_literal: true

module Sourced
  # An "Actor" is an object that holds state, handles commands, produces events, and can also react to events.
  # All capabilities in an app are commands handled by Actors.
  # Actors implement both the Decider interface (to handle commands) and the Reactor interface (to handle events).
  # The top-level API is:
  #   cmd = SomeCommand.new(stream_id: '123', payload: { ... })
  #   state, new_events = SomeActor.handle_command(cmd)
  # 
  # This will:
  # 1. Initialize in-memory state with a stream_id.
  # 2. Load event history from backend for that stream_id.
  # 3. Project the events onto the state, to get at the current state.
  # 4. Yield the state and command to the command handler.
  # 5. Capture any events returned by the command handler.
  # 6. Persist the new events to the backend.
  # 7. Run any synchronous blocks registered with the Actor in the same transaction.
  # 8. Return the updated state and new events.
  #
  # command => Decide[init state, load events, evolve state, handle command, apply events, commit, sync blocks] => [new state, new events]
  #
  # Sourced::Actor defines a DSL to define command handlers, event evolvers and "reactions".
  # See https://ismaelcelis.com/posts/decide-evolve-react-pattern-in-ruby/
  # But Sourced will accept anything that implements the top level Decider API:
  #
  #    .handle_command(Sourced::Command) => [state Any, new_events Array<Sourced::Event>]
  #    .handled_commands() => Array<Sourced::Command>
  #
  # You can also use the #decide(command) => Aray<Events> method directly.
  # for testing behaviour via commands and events without touching the DB.
  #
  #    decider = SomeActor.new('123')
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
  #      # Leads::LeadActor encapsulates working with Leads
  #      class LeadActor < Sourced::Actor
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
  #        # ====== COMMAND BLOCK =================
  #        # Define a command handler for the Leads::CreateLead command
  #        # The command handler takes the current state of the lead
  #        # (which will be the initial state Hash when first creating the lead)
  #        # and the command instance coming from the client or UI or another workflow.
  #        command CreateLead do |state, cmd|
  #          # Apply a LeadCreated event to the state
  #          # events applied here will be stored in the backend
  #          event LeadCreated, name: cmd.payload.name, email: cmd.payload.email
  #
  #          # If the command and event share the same payload attributes, you can just do:
  #          event LeadCreated, cmd.payload
  #        end
  #
  #        # ====== EVENT BLOCK =================
  #        # .event blocks define how an event updates the state
  #        # These blocks are run before running a .command block
  #        # To update the state object from past events
  #        # They're also run within a .command block when applying new events with `#apply`
  #        event LeadCreated do |state, event|
  #          state[:status] = 'created'
  #          state[:name] = event.payload.name
  #          state[:email] = event.payload.email
  #        end
  #
  #        # ====== REACT BLOCK =================
  #        # React blocks listen to events emitted by .command blocks
  #        # From this or any other Actors
  #        # and allow an part of the app to react to events
  #        # This is how you build worlflows that span multiple Actors
  #        # React blocks run in the background, and therefore these workflows
  #        # are eventually consistent, unless configured to be synchronous.
  #        # In this example we listen to LeadCreated events
  #        # and schedule a new SendEmail command
  #        react LeadCreated do |event|
  #          command SendEmail 
  #        end
  #
  #        # We now handle the SendEmail command and the cycle starts again.
  #        command SendEmail do |state, cmd|
  #          raise 'Email already sent' if state[:email_sent]
  #
  #          if Mailer.deliver_to(state[:email], state)
  #            event EmailSent
  #          else
  #            event EmailFailed
  #          end
  #        end
  #
  #        event EmailSent do |state, event|
  #          state[:email_sent] = true
  #        end
  #      end
  #    end
  class Actor
    include Evolve
    include React
    include Sync
    extend Consumer

    PREFIX = 'decide'
    REACTION_WITH_STATE_PREFIX = 'reaction_with_state'

    UndefinedMessageError = Class.new(KeyError)

    # An Actor class has its own Command and Event
    # subclasses that are used to define inine commands and events.
    # These classes serve as message registry for the Actor's inline messages.
    class Command < Sourced::Command; end
    class Event < Sourced::Event; end

    class << self
      def inherited(subclass)
        super
        subclass.const_set(:Command, Class.new(const_get(:Command)))
        subclass.const_set(:Event, Class.new(const_get(:Event)))
        handled_commands.each do |cmd_type|
          subclass.handled_commands << cmd_type
        end
      end

      # Access a Actor's Command or Event classes by name (e.g. :some_command or :some_event)
      # @param message_name [Symbol]
      # @return [Class]
      # @raise [ArgumentError] if the message is not defined
      def [](message_name)
        message_type = __message_type(message_name)
        msg_class = self::Event.registry[message_type] || self::Command.registry[message_type]

        raise UndefinedMessageError, "Message not found: #{message_name}" unless msg_class
        msg_class
      end

      # Define an initial state factory for this decider.
      # @example
      #
      #   state do |id|
      #     { id: id, status: 'new' }
      #   end
      #
      def state(&blk)
        define_method(:init_state) do |id|
          blk.call(id)
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

      # Load a Actor from event history
      #
      # @param stream_id [String] the stream id
      # @return [Actor]
      def load(stream_id, upto: nil)
        new(stream_id).load(upto:)
      end

      def handled_commands
        @handled_commands ||= []
      end

      # Define a command class, register a command handler
      # and define a method to send the command
      # Example:
      #   command :add_item, name: String do |state, cmd|
      #     event(ItemAdded, item_id: SecureRandom.uuid, name: cmd.payload.name)
      #   end
      #
      # # The exmaple above will define a command class `AddItem` in the current namespace:
      # AddItem = Message.define('namespace.add_item', payload_schema: { name: String })
      #
      # # And an :add_item method to send the command:
      # def add_item(name:)
      #   issue_command AddItem, name:
      # end
      #
      # This method can be used on Actor instances:
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
      # @return [Message] the command instance, which can be #valid? or not
      def command(*args, &block)
        raise ArgumentError, 'command block expects signature (state, command)' unless block.arity == 2

        case args
          in [Symbol => cmd_name, Hash => payload_schema]
            __register_named_command_handler(cmd_name, payload_schema, &block)
          in [Symbol => cmd_name]
            __register_named_command_handler(cmd_name, &block)
          in [Class => cmd_type] if cmd_type < Sourced::Message
            __register_class_command_handler(cmd_type, &block)
        else
          raise ArgumentError, "Invalid arguments for #{self}.command"
        end
      end

      private def __register_named_command_handler(cmd_name, payload_schema = nil, &block)
        cmd_class = self::Command.define(__message_type(cmd_name), payload_schema:)
        klass_name = cmd_name.to_s.split('_').map(&:capitalize).join
        const_set(klass_name, cmd_class)
        __register_class_command_handler(cmd_class, &block)
        define_method(cmd_name) do |**payload|
          issue_command cmd_class, payload
        end

        define_method("#{cmd_name}_async") do |**payload|
          cmd = cmd_class.new(stream_id: id, payload:)
          cmd.tap do |c|
            Sourced.schedule_commands([c]) if c.valid?
          end
        end

        define_method("#{cmd_name}_later") do |time, **payload|
          cmd = cmd_class.new(stream_id: id, payload:).delay(time)
          cmd.tap do |c|
            Sourced.schedule_commands([c]) if c.valid?
          end
        end
      end

      private def __register_class_command_handler(cmd_type, &block)
        handled_commands << cmd_type
        define_method(Sourced.message_method_name(PREFIX, cmd_type.name), &block)
      end

      # Support defining event handlers with a symbol and a payload schema
      # Or a class.
      #
      # @example
      #
      #   event SomethingHappened, field1: String do |state, event|
      #     state[:status] = 'done'
      #   end
      #
      #   event :something_happened, field1: String do |state, event|
      #     state[:status] = 'done'
      #   end
      #
      def event(*args, &block)
        case args
          in [Symbol => event_name, Hash => payload_schema]
            __register_named_event_handler(event_name, payload_schema).tap do |event_class|
              super(event_class, &block)
            end
          in [Symbol => event_name]
            __register_named_event_handler(event_name).tap do |event_class|
              super(event_class, &block)
            end
          in [Class => foo]
            super
          else
            raise ArgumentError, "event expects a Symbol or Event class. Got: #{args.inspect}"
        end
      end

      private def __register_named_event_handler(event_name, payload_schema = nil)
        klass_name = event_name.to_s.split('_').map(&:capitalize).join
        event_class = self::Event.define(__message_type(event_name), payload_schema:)
        const_set(klass_name, event_class)
      end

      # Support defining event reactions with a symbol
      # pointing to an event class defined locally with .event
      # See Sourced::React#reaction
      # TODO: should this be defined in Evolve?
      # @example
      #   reaction :item_added do |event|
      #     stream = stream_for(event)
      #     stream.command :do_something
      #   end
      #
      #   reaction ItemAdded do |event|
      #     stream = stream_for(event)
      #     stream.command DoSomething
      #   end
      #
      # @param event_name [Symbol, Class]
      # @yield [Sourced::Event]
      # @return [void]
      def reaction(event_name, &block)
        if event_name.is_a?(Symbol)
          event_class = self::Event.registry[__message_type(event_name)]
          super(event_class, &block)
        else
          super
        end
      end

      # React to events this class evolves from.
      # And load internal state from past events 
      # So that Actor can react to own events and have state available.
      # This should be useful to implement TODO List - style reactors that can keep their own state based on events.
      #
      # @example
      #
      #  event SomethingHappened do |state, event|
      #    state[:count] += 1
      #  end
      #
      #  react_with_state SomethingHappened do |state, event|
      #    if state[:count] % 3 == 0
      #      command NotifyEachThirdTime
      #    end
      #  end
      #
      # Note: this callback loads the decider's state _up to its current state_, which
      # may be more recent than what the event reacted to represents.
      # Ex. if the event history of the decider is [E1, E2, E3, E4]
      # a reaction registered with `.reaction_with_state(E2, &block)` will run asynchronously _later_.
      # It will react to E2 even though the decider's current state is up to E4.
      # It's up to the reaction block to take that into account, for example comparing the decider's and the event's sequence number.
      #
      #  reaction_with_state SomethingHappened do |state, event|
      #    if seq == event.seq # event is the last one to have happened.
      #      command DoSomething
      #    else
      #      # ignore ?
      #    end
      #  end
      #
      # @param event_name [Symbol, Class]
      # @yield [Object, Sourced::Event]
      # @return [void]
      def reaction_with_state(event_name, &block)
        raise ArgumentError, 'react_with_state expects a block' unless block_given?

        event_class = if event_name.is_a?(Symbol)
          self::Event.registry[__message_type(event_name)]
        else
          event_name
        end
        raise ArgumentError, '.react_with_state expects a block with |state, event|' unless block.arity == 2
        unless handled_events_for_evolve.include?(event_class)
          raise ArgumentError, '.react_with_state only works with event types handled by this class via .event(event_type)' 
        end

        method_name = Sourced.message_method_name(REACTION_WITH_STATE_PREFIX, event_class.to_s)
        define_method(method_name, &block)
        send(:private, method_name)

        reaction event_class do |event|
          load(after: seq)
          send(method_name, state, event)
        end
      end

      # Override the default namespace for commands and events
      # defined inline
      # @example
      #
      #   def message_namespace = 'my_app.messages.'
      #
      # @return [String]
      def message_namespace
        __string_to_message_type(name)
      end

      private def __message_type_prefix
        @__message_type_prefix ||= message_namespace
      end

      def __message_type(msg_name)
        [__message_type_prefix, msg_name].join('.').downcase
      end

      def __string_to_message_type(str)
        str.to_s.gsub(/::/, '.')
          .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
          .gsub(/([a-z\d])([A-Z])/, '\1_\2')
          .tr("-", "_")
          .downcase
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
      @__current_command = Sourced::Command.new(stream_id: id)
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

    # Apply an event from within a command handler
    # @example
    #
    #  command DoSomething do |state, cmd|
    #    event SomethingHappened, field1: 'foo', field2: 'bar'
    #  end
    #
    # Or, with symbol pointing to an event class defined with .event
    #   command DoSomething do |state, cmd|
    #     event :something_happened, field1: 'foo', field2: 'bar'
    #   end
    #
    # @param event_name [Symbol, Class] the event name or class
    # @param payload [Hash] the event payload
    # @return [Any] the
    def event(event_name, payload = {})
      return apply(event_name, payload) unless event_name.is_a?(Symbol)

      event_class = Event.registry[self.class.__message_type(event_name)]
      raise ArgumentError, "Event not found: #{event_name}" unless event_class

      apply(event_class, payload)
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
    private def apply(event_class, payload = {})
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
    # SYNC reactors that are listening to events emitted by this Actor.
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
    # to their target Actor
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
