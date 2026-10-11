# frozen_string_literal: true

module Sourced
  # Reactor base class for state machines: Sourced's take on statecharts.
  #
  # A machine's position is defined by *checkpoints*: event classes declared
  # with {.checkpoint} or {.final}. The machine is at its latest checkpoint;
  # every other event it evolves is data, and only changes its domain +state+.
  # Each checkpoint lists the commands it accepts, and the checkpoints each
  # command may produce:
  #
  #   class OrderMachine < Sourced::Machine
  #     partition_by :order_id
  #
  #     state { |keys| OrderState.new(keys[:order_id]) }
  #     evolve(ItemAdded) { |order, evt| order.add(evt.payload.sku) }
  #
  #     initial do
  #       command PlaceOrder => OrderPlaced          # appends OrderPlaced from the command's payload
  #     end
  #
  #     checkpoint OrderPlaced do
  #       entry { |order| dispatch ChargeCard, total: order.total }
  #       command AddItem do |order, cmd|           # no arrow: data only, stays at OrderPlaced
  #         event ItemAdded, sku: cmd.payload.sku
  #       end
  #       command Pay => OrderPaid | PaymentDeclined do |order, cmd|
  #         cmd.payload.ok ? event(OrderPaid) : event(PaymentDeclined)
  #       end
  #     end
  #
  #     final OrderPaid
  #     checkpoint PaymentDeclined
  #   end
  #
  # A command decides into exactly one of its declared checkpoints, or into none
  # when the declaration includes +nil+ (or has no arrow). Commands the current
  # checkpoint doesn't accept are refused: see {#handles_command?}.
  #
  # Like a {Decider}, a machine is one reactor and one consumer group: every
  # message for a partition is decided one at a time, under one consistency
  # guard. Entry and exit actions are reactions: they run when the machine
  # claims the checkpoint event, after the deciding command has committed.
  class Machine
    include Sourced::Evolve
    include Sourced::React
    include Sourced::Sync
    extend Sourced::Consumer

    Error = Class.new(Sourced::Error)

    # Raised when a machine's definition is invalid. Checked when the machine is
    # registered with a Router, and before its first instance is built.
    DefinitionError = Class.new(Error)

    # Raised when a decision emits checkpoints its command didn't declare.
    CheckpointError = Class.new(Error)

    # Raised when a machine builds an event that isn't valid, e.g. a checkpoint
    # auto-appended from a command that lacks one of its required attributes.
    InvalidEventError = Class.new(Error)

    INITIAL = :initial

    # Metadata key on messages dispatched by an +after+ block: the id of the
    # checkpoint event that started the timer.
    ENTERED_BY = :entered_by

    # A command a checkpoint accepts, and the checkpoints it may decide into.
    # +nil+ among the targets means "may emit no checkpoint".
    CommandDef = Data.define(:command_class, :targets, :block) do
      # A single checkpoint is appended for the handler when it doesn't emit it.
      def auto? = targets.size == 1 && !targets.first.nil?
      def optional? = targets.include?(nil)
      def checkpoint_targets = targets.compact
    end

    # A delayed action: runs when its checkpoint is entered, and future-dates
    # what it dispatches by +delay+ seconds from the entering event.
    After = Data.define(:delay, :block)

    # A checkpoint (or the initial position) and what it accepts.
    class Node
      attr_reader :key, :commands, :entries, :exits, :afters

      # @param key [Class, Symbol] the checkpoint event class, or {INITIAL}
      # @param final [Boolean]
      def initialize(key, final:)
        @key = key
        @final = final
        @commands = {}
        @entries = []
        @exits = []
        @afters = []
      end

      def final? = @final
      def initial? = key == INITIAL
    end

    # Evaluates a checkpoint's block: +command+, +entry+, +exit+.
    class NodeBuilder
      def initialize(machine, node)
        @machine = machine
        @node = node
      end

      # @overload command(command_class => target, &block)
      #   @param target [Class, Plumb::Or, nil] a checkpoint, a union of them, or +nil+
      # @overload command(command_class, &block)
      #   Data only: same as +command_class => nil+.
      def command(declaration, &block)
        definition = @machine.send(:__command_definition, declaration, block)
        if @node.commands.key?(definition.command_class)
          raise DefinitionError, "#{@machine}: #{node_label} accepts #{definition.command_class} twice"
        end

        @node.commands[definition.command_class] = definition
      end

      # Runs when the machine enters this checkpoint, as a reaction.
      # @yield [state, checkpoint_event]
      def entry(&block)
        raise DefinitionError, "#{@machine}: the initial position has no event, so no entry actions" if @node.initial?

        @node.entries << block
      end

      # Runs when the machine leaves this checkpoint, as a reaction to the
      # checkpoint that replaced it.
      # @yield [state, replacing_checkpoint_event]
      def exit(&block) = @node.exits << block

      # Runs when the machine enters this checkpoint, as a reaction. Messages it
      # dispatches are delivered +delay+ seconds after the entering event, and
      # carry that event's id: a command delivered after the machine has moved
      # to a newer checkpoint is stale, and refused (see {Machine#handles_command?}).
      #
      #   checkpoint PaymentStarted do
      #     after(3600) { |_state, _evt| dispatch ExpirePayment }
      #     command ExpirePayment => PaymentExpired
      #   end
      #
      # @param delay [Numeric] seconds
      # @yield [state, checkpoint_event]
      def after(delay, &block)
        raise DefinitionError, "#{@machine}: the initial position has no event, so no timers" if @node.initial?
        raise DefinitionError, "#{@machine}: after needs a delay in seconds, got #{delay.inspect}" unless delay.is_a?(Numeric)
        raise DefinitionError, "#{@machine}: after(#{delay}) needs a block" unless block

        @node.afters << After.new(delay:, block:)
      end

      private

      def node_label = @node.initial? ? 'initial' : @node.key.to_s
    end

    # Read-only view of a machine's runtime state, available as {#machine}.
    View = Data.define(:value, :entered_by, :final) do
      # @param checkpoint [Class, Symbol] a checkpoint event class, or +:initial+
      def in?(checkpoint) = value == checkpoint
      def final? = final
    end

    # A loaded machine, as returned by {.load}.
    # +value+ is the current checkpoint class (or +:initial+); +state+ is the domain state.
    Snapshot = Data.define(:value, :state)

    # The machine's own bookkeeping, separate from the domain state.
    class Runtime
      attr_reader :current, :entered_by

      def initialize
        @current = INITIAL
        @entered_by = nil
        @replaced = {}
      end

      # @param checkpoint [Sourced::Message] a checkpoint event, in log order
      def enter(checkpoint)
        @replaced[checkpoint.id] = @current
        @current = checkpoint.class
        @entered_by = checkpoint
      end

      # The checkpoint +checkpoint+ replaced when it was evolved.
      # @return [Class, Symbol, nil]
      def replaced_by(checkpoint) = @replaced[checkpoint.id]
    end

    class << self
      # @return [Hash{Class, Symbol => Node}] checkpoint nodes, keyed by event class or {INITIAL}
      def nodes = @nodes ||= {}

      # The machine's position before its first checkpoint.
      def initial(&block) = __define_node(INITIAL, final: false, &block)

      # Declare a checkpoint, and the commands it accepts.
      # @param event_class [Class]
      def checkpoint(event_class, &block) = __define_node(event_class, final: false, &block)

      # Declare a final checkpoint. It accepts no commands.
      # @param event_class [Class]
      def final(event_class, &block) = __define_node(event_class, final: true, &block)

      # @return [Array<Class>] declared checkpoint event classes
      def checkpoint_classes = nodes.keys.grep(Class)

      # @param klass [Class]
      def checkpoint?(klass) = klass.is_a?(Class) && nodes.key?(klass)

      # @return [Array<Class>] every command any checkpoint accepts
      def handled_commands = nodes.values.flat_map { |node| node.commands.keys }.uniq

      # Claimed: commands to decide, checkpoints to run entry and exit actions
      # for, and any events with a +reaction+. Every checkpoint is claimed, with
      # or without actions: claiming them is also how a machine notices
      # checkpoints appended by other reactors, which joins decide on.
      def handled_messages = (handled_commands + checkpoint_classes + handled_messages_for_react).uniq

      # Check the definition. Memoised; runs before the first instance is built,
      # and when the machine is registered with a Router.
      #
      # @raise [DefinitionError]
      # @return [true]
      def validate!
        @validated ||= begin
          __validate!
          true
        end
      end

      # Build executable actions for a claimed batch.
      #
      # +history+ comes from an unbounded read over the machine's evolve types,
      # so it holds the claimed checkpoint events (and anything appended since),
      # in log order. The machine evolves all of it: commands are decided against
      # the latest state, and checkpoint events run the exit and entry actions of
      # the checkpoints they replaced and entered.
      #
      # @param claim [ClaimResult]
      # @param history [ReadResult]
      # @return [Array<Array(Array<Object>, PositionedMessage)>] action/source pairs
      def handle_claim(claim, history:)
        values = partition_keys.to_h { |k| [k, claim.partition_value[k.to_s]] }
        __process(values, claim.messages, history.messages, guard: history.guard, replaying: claim.replaying)
      end

      # Build executable actions for +new_messages+, given prior +history+
      # disjoint from them (the GWT helpers' contract). The events among
      # +new_messages+ are evolved after +history+.
      #
      # @param partition_values [Hash{Symbol => String}]
      # @param new_messages [Array<Sourced::Message>]
      # @param history [ReadResult] prior history
      # @param replaying [Boolean] forwarded to {React#should_react?}, which by
      #   default skips entry and exit actions when +true+
      # @return [Array<Array(Array<Object>, Sourced::Message)>] action/source pairs
      def handle_batch(partition_values, new_messages, history:, replaying: false)
        evolvable = new_messages.select { |msg| handled_messages_for_evolve.include?(msg.class) }
        __process(partition_values, new_messages, history.messages + evolvable, guard: history.guard, replaying:)
      end

      # Load a machine instance and return its position and domain state.
      #
      # @param store [Sourced::Store, nil] defaults to +Sourced.store+
      # @return [Snapshot]
      def load(store: nil, **values)
        instance, = Sourced.load(self, store:, **values)
        Snapshot.new(value: instance.machine.value, state: instance.state)
      end

      private

      def __define_node(key, final:, &block)
        raise DefinitionError, "#{self}: #{key} is declared twice" if nodes.key?(key)
        unless key == INITIAL || (key.is_a?(Class) && key < Sourced::Message)
          raise DefinitionError, "#{self}: checkpoints are message classes, got #{key.inspect}"
        end

        node = Node.new(key, final:)
        nodes[key] = node
        # Checkpoints are always evolved, alongside any +evolve+ types.
        handled_messages_for_evolve << key if key.is_a?(Class) && !handled_messages_for_evolve.include?(key)
        NodeBuilder.new(self, node).instance_eval(&block) if block
        node
      end

      def __command_definition(declaration, block)
        command_class, targets =
          case declaration
          in Hash if declaration.size == 1
            [declaration.keys.first, __flatten_targets(declaration.values.first)]
          in Class
            [declaration, [nil]]
          else
            raise DefinitionError, "#{self}: invalid command declaration #{declaration.inspect}"
          end

        unless command_class.is_a?(Class) && command_class < Sourced::Message
          raise DefinitionError, "#{self}: commands are message classes, got #{command_class.inspect}"
        end
        if block.nil? && (targets.size > 1 || targets == [nil])
          raise DefinitionError,
                "#{self}: #{command_class} => #{targets.map { |t| t || 'nil' }.join(' | ')} " \
                'needs a block to emit its checkpoint'
        end

        CommandDef.new(command_class:, targets: targets.uniq, block:)
      end

      # @return [Array<Class, nil>]
      def __flatten_targets(target)
        case target
        when nil then [nil]
        when Class then [target]
        when Plumb::Or then target.children.flat_map { |child| __flatten_targets(child) }
        when Plumb::ValueClass
          raise DefinitionError, "#{self}: invalid checkpoint #{target.inspect}" unless target.children == [nil]

          [nil]
        else
          raise DefinitionError, "#{self}: invalid checkpoint #{target.inspect}"
        end
      end

      def __validate!
        raise DefinitionError, "#{self} needs partition_by" if partition_keys.empty?

        checkpoint_classes.each { |klass| __check_partition_keys!(klass, 'checkpoint') }
        nodes.each_value do |node|
          if node.final? && node.commands.any?
            raise DefinitionError, "#{self}: final checkpoint #{node.key} can't accept commands"
          end

          node.commands.each_value do |definition|
            __check_partition_keys!(definition.command_class, 'command')
            definition.checkpoint_targets.each do |target|
              unless checkpoint?(target)
                raise DefinitionError, "#{self}: #{definition.command_class} targets #{target}, which isn't a checkpoint"
              end
            end
            __check_buildable!(definition) if definition.auto? && definition.block.nil?
          end
        end
      end

      def __check_partition_keys!(klass, role)
        names = __attribute_names(klass)
        missing = partition_keys - names
        return if missing.empty?

        raise DefinitionError, "#{self}: #{role} #{klass} must declare #{missing.join(', ')}"
      end

      # A blockless command's checkpoint is built from the command's payload, so
      # every attribute the command can't supply must be allowed to be missing.
      def __check_buildable!(definition)
        checkpoint = definition.targets.first
        return unless checkpoint.const_defined?(:Payload, false)

        supplied = __attribute_names(definition.command_class) + partition_keys
        checkpoint::Payload._schema._schema.each do |key, type|
          next if supplied.include?(key.to_sym)
          next if key.optional? || type.resolve(Plumb::Undefined).valid?

          raise DefinitionError,
                "#{self}: #{definition.command_class} => #{checkpoint} has no block, " \
                "and #{checkpoint} requires #{key.to_sym}, which #{definition.command_class} doesn't declare"
        end
      end

      def __attribute_names(klass)
        klass.respond_to?(:payload_attribute_names) ? klass.payload_attribute_names : []
      end

      def __process(values, messages, full_history, guard:, replaying:)
        instance = new(values)
        instance.evolve(full_history)

        each_with_partial_ack(messages) do |msg|
          if handled_commands.include?(msg.class)
            next [Actions::OK, msg] unless instance.handles_command?(msg)

            events = instance.decide(msg)
            actions = Actions.build_for(events, guard:, source: msg)
            # The runner correlates the events as it appends them; +events:+ is
            # bound then, so the hooks see the events as stored.
            actions += instance.collect_actions(state: instance.state, messages: [msg]) do |appended|
              { events: appended }
            end
            [actions, msg]
          elsif (checkpoint?(msg.class) || instance.reacts_to?(msg)) &&
                instance.should_react?(instance.state, msg, replaying:)
            reaction_msgs = instance.__react(msg)
            actions = Actions.build_for(reaction_msgs, source: msg)
            actions += instance.collect_actions(state: instance.state, messages: [msg], events: [])
            [actions, msg]
          else
            [Actions::OK, msg]
          end
        end
      end
    end

    attr_reader :partition_values

    # @param partition_values [Hash{Symbol => String}]
    def initialize(partition_values = {})
      self.class.validate!
      @partition_values = partition_values
      @runtime = Runtime.new
      @uncommitted_events = nil
    end

    # Read-only view of the runtime state: the current checkpoint, the event
    # that entered it, and whether it's final.
    # @return [View]
    def machine
      View.new(value: @runtime.current, entered_by: @runtime.entered_by, final: current_node.final?)
    end

    # Evolve messages in log order: checkpoints move the machine, then any
    # +evolve+ handler updates the domain state.
    def evolve(messages)
      Array(messages).each do |msg|
        @runtime.enter(msg) if self.class.checkpoint?(msg.class)
        super([msg])
      end
      state
    end

    # Whether the current checkpoint accepts +command+. A command dispatched by
    # an +after+ block is refused once the machine has left the checkpoint
    # entry that started it, even if the current checkpoint accepts it.
    #
    # {Sourced.handle!} raises {Sourced::CommandNotHandledError} when it doesn't,
    # appending nothing; in background workers the command is acked with no events.
    #
    # @param command [Sourced::Message]
    # @return [Boolean]
    def handles_command?(command)
      return false if stale_timer?(command)

      !command_definition_for(command).nil?
    end

    # Decide +command+ against the current checkpoint and state.
    #
    # Runs the command's handler, appends its checkpoint when the declaration
    # names a single one the handler didn't emit, and checks the decision
    # emitted exactly one of the declared checkpoints (or none, when +nil+ is
    # declared).
    #
    # @param command [Sourced::Message]
    # @return [Array<Sourced::Message>] the events produced
    # @raise [CheckpointError] if the decision emitted other checkpoints
    def decide(command)
      definition = command_definition_for(command)
      return [] unless definition

      @uncommitted_events = []
      instance_exec(state, command, &definition.block) if definition.block
      emitted = @uncommitted_events.select { |evt| self.class.checkpoint?(evt.class) }
      emitted << event(definition.targets.first, command.payload&.to_h || {}) if emitted.empty? && definition.auto?
      check_checkpoints!(definition, emitted)
      @uncommitted_events
    ensure
      @uncommitted_events = nil
    end

    # Produce an event from within a command handler and apply it immediately.
    # Partition keys the event declares are filled in.
    #
    # @param event_class [Class, Symbol]
    # @param payload [Hash]
    # @return [Sourced::Message]
    # @raise [InvalidEventError] if the event isn't valid
    def event(event_class, payload = {})
      raise Error, "#{self.class}: events are produced by command handlers only" unless @uncommitted_events

      event_class = self.class[event_class] if event_class.is_a?(Symbol)
      evt = event_class.new(payload: with_partition_keys(event_class, payload))
      raise InvalidEventError, "#{self.class}: invalid #{event_class}: #{evt.errors.inspect}" unless evt.valid?

      @uncommitted_events << evt
      evolve([evt])
      evt
    end

    # Run the exit actions of the checkpoint +msg+ replaced and the entry actions
    # of +msg+'s checkpoint, then any +reaction+ to it.
    # @api private
    # @return [Array<Sourced::Message>] dispatched messages
    def __react(msg)
      __handling_reactions([msg]) do |message|
        if self.class.checkpoint?(message.class)
          left = self.class.nodes[@runtime.replaced_by(message)]
          left&.exits&.each { |blk| instance_exec(state, message, &blk) }
          entered = self.class.nodes.fetch(message.class)
          entered.entries.each { |blk| instance_exec(state, message, &blk) }
          start_timers(entered, message)
        end

        method_name = Sourced.message_method_name(React::PREFIX, message.class.to_s)
        respond_to?(method_name) ? Array(send(method_name, state, message)).compact : EMPTY_ARRAY
      end
    end

    private

    # Timers for a checkpoint the machine has already left would only be stale
    # when delivered, so they aren't started.
    def start_timers(node, checkpoint)
      return unless @runtime.entered_by&.id == checkpoint.id

      node.afters.each do |timer|
        started = @__reaction_dispatchers.size
        instance_exec(state, checkpoint, &timer.block)
        @__reaction_dispatchers.drop(started).each do |dispatcher|
          dispatcher.at(checkpoint.created_at + timer.delay).with_metadata(ENTERED_BY => checkpoint.id)
        end
      end
    end

    # A timer command was caused by the checkpoint event it names: dispatched in
    # reaction to it. Metadata is copied along correlation chains, so a command
    # that merely inherited the key names an event other than its cause, and
    # isn't a timer.
    def stale_timer?(command)
      started_by = command.metadata[ENTERED_BY] || command.metadata[ENTERED_BY.to_s]
      return false unless started_by && started_by == command.causation_id

      started_by != @runtime.entered_by&.id
    end

    def current_node = self.class.nodes.fetch(@runtime.current) { Node.new(@runtime.current, final: false) }

    def command_definition_for(command) = current_node.commands[command.class]

    # Partition keys are filled in on dispatched messages too.
    def dispatch(message_class, payload = {})
      message_class = self.class[message_class] if message_class.is_a?(Symbol)
      super(message_class, with_partition_keys(message_class, payload))
    end

    def with_partition_keys(message_class, payload)
      names = message_class.respond_to?(:payload_attribute_names) ? message_class.payload_attribute_names : []
      keys = self.class.partition_keys.select { |k| names.include?(k) }.to_h { |k| [k, partition_values[k]] }
      keys.merge(payload.to_h)
    end

    def check_checkpoints!(definition, emitted)
      declared = definition.targets.map { |t| t || 'nil' }.join(' | ')
      if emitted.size > 1
        raise CheckpointError, "#{self.class}: #{definition.command_class} emitted #{emitted.map(&:class).join(', ')}; " \
                               "a decision emits one checkpoint (#{declared})"
      elsif emitted.empty?
        return if definition.optional?

        raise CheckpointError, "#{self.class}: #{definition.command_class} must emit one of #{declared}"
      elsif !definition.checkpoint_targets.include?(emitted.first.class)
        raise CheckpointError, "#{self.class}: #{definition.command_class} emitted #{emitted.first.class}, not one of #{declared}"
      end
    end
  end
end
