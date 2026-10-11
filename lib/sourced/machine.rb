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
  #       command CancelOrder => OrderCancelled
  #
  #       region :payment do                         # regions are active while OrderPlaced is
  #         initial do
  #           command Pay => PaymentCaptured | PaymentDeclined do |order, cmd|
  #             cmd.payload.ok ? event(PaymentCaptured) : event(PaymentDeclined)
  #           end
  #         end
  #         final PaymentCaptured
  #         checkpoint PaymentDeclined
  #       end
  #
  #       region :shipping do
  #         initial { command Ship => Shipped }
  #         final Shipped
  #       end
  #
  #       done OrderCompleted                        # appended once every region is final
  #     end
  #
  #     final OrderCompleted
  #     final OrderCancelled
  #   end
  #
  # A command decides into exactly one of its declared checkpoints, or into none
  # when the declaration includes +nil+ (or has no arrow). Commands no active
  # checkpoint accepts are refused: see {#handles_command?}.
  #
  # Like a {Decider}, a machine is one reactor and one consumer group: every
  # message for a partition is decided one at a time, under one consistency
  # guard, so regions never run concurrently and share the domain state. Entry
  # and exit actions are reactions: they run when the machine claims the
  # checkpoint event, after the deciding command has committed.
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

    # Joins chain (a join's checkpoint can complete an outer join), but a
    # definition can't make them chain forever. A safety net only.
    MAX_JOINS = 100

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

    # A set of checkpoints, one of them current while the region is active.
    # The root region is always active; any other belongs to a checkpoint, and
    # is active while that checkpoint is.
    class Region
      attr_reader :name, :parent, :nodes

      # @param name [Symbol]
      # @param parent [Node, nil] the checkpoint the region belongs to; nil for the root
      def initialize(name, parent)
        @name = name
        @parent = parent
        @nodes = {}
      end

      def root? = parent.nil?

      # The region's position before its first checkpoint, declared or not.
      def initial_node = nodes[INITIAL] || (@blank_initial ||= Node.new(INITIAL, final: false, region: self))

      # @return [Array<Region>] this region and its ancestors, innermost first
      def lineage = root? ? [self] : [self, *parent.region.lineage]

      def label = root? ? 'the root region' : "region #{name.inspect}"
    end

    # A checkpoint (or a region's initial position) and what it accepts.
    class Node
      attr_reader :key, :region, :commands, :entries, :exits, :afters, :regions
      attr_accessor :done

      # @param key [Class, Symbol] the checkpoint event class, or {INITIAL}
      # @param final [Boolean]
      # @param region [Region] the region the checkpoint belongs to
      def initialize(key, final:, region:)
        @key = key
        @final = final
        @region = region
        @commands = {}
        @entries = []
        @exits = []
        @afters = []
        @regions = []
        @done = nil
      end

      def final? = @final
      def initial? = key == INITIAL
      def label = initial? ? "#{region.label}'s initial position" : key.to_s

      # Whether this checkpoint is inside +other+'s regions, at any depth.
      def descendant_of?(other)
        parent = region.parent
        !parent.nil? && (parent == other || parent.descendant_of?(other))
      end
    end

    # Evaluates a region's block: +initial+, +checkpoint+, +final+.
    class RegionBuilder
      def initialize(machine, region)
        @machine = machine
        @region = region
      end

      def initial(&block) = @machine.send(:__define_node, @region, INITIAL, final: false, &block)
      def checkpoint(event_class, &block) = @machine.send(:__define_node, @region, event_class, final: false, &block)
      def final(event_class, &block) = @machine.send(:__define_node, @region, event_class, final: true, &block)
    end

    # Evaluates a checkpoint's block: +command+, +entry+, +exit+, +after+, +region+, +done+.
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
          raise DefinitionError, "#{@machine}: #{@node.label} accepts #{definition.command_class} twice"
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
      # checkpoint that replaced it (in this region, or an ancestor's).
      # @yield [state, replacing_checkpoint_event]
      def exit(&block) = @node.exits << block

      # Runs when the machine enters this checkpoint, as a reaction. Messages it
      # dispatches are delivered +delay+ seconds after the entering event, and
      # carry that event's id: a command delivered after the machine has left
      # that checkpoint entry is stale, and refused (see {Machine#handles_command?}).
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

      # Declare a region, active while this checkpoint is. A checkpoint with
      # regions is parallel: all of its regions are active at once.
      # @param name [Symbol] unique within the machine
      def region(name, &block)
        raise DefinitionError, "#{@machine}: the initial position can't have regions" if @node.initial?
        raise DefinitionError, "#{@machine}: final checkpoint #{@node.key} can't have regions" if @node.final?

        @machine.send(:__define_region, @node, name.to_sym, &block)
      end

      # Join: once every region of this checkpoint is at a final checkpoint,
      # the machine appends +event_class+, in the same decision when one of its
      # commands completes the last region, or when it claims a checkpoint
      # appended elsewhere that does.
      # @param event_class [Class] a checkpoint of this region or an ancestor's
      def done(event_class)
        raise DefinitionError, "#{@machine}: #{@node.label} declares done twice" if @node.done

        @node.done = event_class
      end
    end

    # Read-only view of a machine's runtime state, available as {#machine}.
    class View
      def initialize(runtime) = @runtime = runtime

      # The current configuration: a checkpoint class (or +:initial+), or for a
      # checkpoint with regions, +{ checkpoint => { region_name => value, … } }+.
      def value = @runtime.value

      # @overload in?(checkpoint)
      #   Whether +checkpoint+ is active, in any region.
      # @overload in?(region_name, checkpoint)
      #   Whether +region_name+ is active and at +checkpoint+ (or +:initial+).
      def in?(*args)
        case args
        in [checkpoint]
          @runtime.active_nodes.any? { |node| node.key == checkpoint }
        in [Symbol => region_name, checkpoint]
          region = @runtime.active_regions.find { |r| r.name == region_name }
          !region.nil? && @runtime.current_node(region).key == checkpoint
        end
      end

      # Whether the root region is at a final checkpoint.
      def final? = @runtime.current_node(@runtime.root).final?

      # The event that entered the current checkpoint of the root region, or of
      # +region_name+ (nil while it's at its initial position or inactive).
      def entered_by(region_name = nil)
        region = region_name ? @runtime.active_regions.find { |r| r.name == region_name } : @runtime.root
        region && @runtime.entered_by(region)
      end
    end

    # A loaded machine, as returned by {.load}.
    # +value+ is the current configuration (see {View#value}); +state+ is the domain state.
    Snapshot = Data.define(:value, :state)

    # The machine's own bookkeeping, separate from the domain state: the
    # current checkpoint and its entering event, per region.
    class Runtime
      Position = Struct.new(:current, :entered_by)

      attr_reader :root

      def initialize(root)
        @root = root
        @positions = {}
        @exited = {}
        @ignored = {}
      end

      def current_node(region)
        key = position(region).current
        key == INITIAL ? region.initial_node : region.nodes.fetch(key)
      end

      def entered_by(region) = position(region).entered_by

      def active?(region)
        region.root? || (active?(region.parent.region) && position(region.parent.region).current == region.parent.key)
      end

      def active_node?(node) = active?(node.region) && current_node(node.region) == node

      # @return [Array<Node>] the active checkpoints, outermost first, regions in definition order
      def active_nodes = active_nodes_in(root)

      # @return [Array<Region>] the active regions, outermost first, in definition order
      def active_regions = active_nodes.flat_map(&:regions).unshift(root)

      # Whether +event_id+ entered a checkpoint that's still active.
      def active_entry?(event_id)
        active_regions.any? { |region| position(region).entered_by&.id == event_id }
      end

      def value = value_of(root)

      # Apply a checkpoint event, in log order. A checkpoint of an inactive
      # region is ignored: a region's history starts when its parent checkpoint
      # is entered.
      #
      # @param event [Sourced::Message]
      # @param node [Node] the checkpoint's node
      def enter(event, node)
        unless active?(node.region)
          @ignored[event.id] = true
          return
        end

        @exited[event.id] = exits_from(current_node(node.region))
        pos = position(node.region)
        pos.current = node.key
        pos.entered_by = event
        # A fresh activation of the checkpoint's regions
        node.regions.each { |region| reset(region) }
      end

      # The checkpoints +event+ made the machine leave, innermost first.
      # @return [Array<Node>]
      def exited_by(event) = @exited.fetch(event.id) { [] }

      def ignored?(event) = @ignored.key?(event.id)

      private

      def position(region) = @positions[region] ||= Position.new(INITIAL, nil)

      def active_nodes_in(region)
        node = current_node(region)
        [node, *node.regions.flat_map { |child| active_nodes_in(child) }]
      end

      # Leaving +node+ leaves its active descendants first.
      def exits_from(node)
        node.regions.flat_map { |region| exits_from(current_node(region)) } + [node]
      end

      def reset(region)
        @positions[region] = Position.new(INITIAL, nil)
        region.nodes.each_value { |node| node.regions.each { |child| reset(child) } }
      end

      def value_of(region)
        node = current_node(region)
        return node.key if node.regions.empty?

        { node.key => node.regions.to_h { |child| [child.name, value_of(child)] } }
      end
    end

    class << self
      # The root region: the machine's top-level checkpoints.
      # @return [Region]
      def root = @root ||= Region.new(:root, nil)

      # @return [Hash{Class => Node}] every checkpoint, in any region
      def checkpoints = @checkpoints ||= {}

      # @return [Hash{Symbol => Region}] every region but the root
      def regions = @regions ||= {}

      # The machine's position before its first checkpoint.
      def initial(&block) = __define_node(root, INITIAL, final: false, &block)

      # Declare a checkpoint, and the commands it accepts.
      # @param event_class [Class]
      def checkpoint(event_class, &block) = __define_node(root, event_class, final: false, &block)

      # Declare a final checkpoint. It accepts no commands.
      # @param event_class [Class]
      def final(event_class, &block) = __define_node(root, event_class, final: true, &block)

      # @return [Array<Class>] declared checkpoint event classes
      def checkpoint_classes = checkpoints.keys

      # @param klass [Class]
      def checkpoint?(klass) = checkpoints.key?(klass)

      # @return [Array<Node>] every node, initial positions included
      def all_nodes = [root, *regions.values].flat_map { |region| region.nodes.values }

      # @return [Array<Class>] every command any checkpoint accepts
      def handled_commands = all_nodes.flat_map { |node| node.commands.keys }.uniq

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
      # the checkpoints they left and entered.
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
      #   default skips entry and exit actions, timers and joins when +true+
      # @return [Array<Array(Array<Object>, Sourced::Message)>] action/source pairs
      def handle_batch(partition_values, new_messages, history:, replaying: false)
        evolvable = new_messages.select { |msg| handled_messages_for_evolve.include?(msg.class) }
        __process(partition_values, new_messages, history.messages + evolvable, guard: history.guard, replaying:)
      end

      # Load a machine instance and return its configuration and domain state.
      #
      # @param store [Sourced::Store, nil] defaults to +Sourced.store+
      # @return [Snapshot]
      def load(store: nil, **values)
        instance, = Sourced.load(self, store:, **values)
        Snapshot.new(value: instance.machine.value, state: instance.state)
      end

      private

      def __define_node(region, key, final:, &block)
        unless key == INITIAL || (key.is_a?(Class) && key < Sourced::Message)
          raise DefinitionError, "#{self}: checkpoints are message classes, got #{key.inspect}"
        end
        if region.nodes.key?(key) || checkpoints.key?(key)
          raise DefinitionError, "#{self}: #{key == INITIAL ? "#{region.label}'s initial position" : key} is declared twice"
        end

        node = Node.new(key, final:, region:)
        region.nodes[key] = node
        unless key == INITIAL
          checkpoints[key] = node
          # Checkpoints are always evolved, alongside any +evolve+ types.
          handled_messages_for_evolve << key unless handled_messages_for_evolve.include?(key)
        end
        NodeBuilder.new(self, node).instance_eval(&block) if block
        node
      end

      def __define_region(node, name, &block)
        raise DefinitionError, "#{self}: region #{name.inspect} is declared twice" if regions.key?(name)

        region = Region.new(name, node)
        regions[name] = region
        node.regions << region
        RegionBuilder.new(self, region).instance_eval(&block) if block
        region
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
        all_nodes.each do |node|
          if node.final? && node.commands.any?
            raise DefinitionError, "#{self}: final checkpoint #{node.key} can't accept commands"
          end

          node.commands.each_value do |definition|
            __check_partition_keys!(definition.command_class, 'command')
            definition.checkpoint_targets.each do |target|
              __check_reachable!(node, target, "#{definition.command_class} targets")
            end
            if definition.auto? && definition.block.nil?
              __check_buildable!(definition.targets.first, definition.command_class)
            end
          end

          __check_join!(node) if node.done || node.regions.any?
        end
      end

      # A checkpoint can move the machine within its own region, or out to an
      # ancestor's; never into a sibling region.
      def __check_reachable!(node, target, what)
        target_node = checkpoints[target]
        raise DefinitionError, "#{self}: #{what} #{target}, which isn't a checkpoint" unless target_node
        return if node.region.lineage.include?(target_node.region)

        raise DefinitionError,
              "#{self}: #{what} #{target}, a checkpoint of #{target_node.region.label}; " \
              "#{node.label} can only reach #{node.region.label} or its ancestors"
      end

      def __check_join!(node)
        raise DefinitionError, "#{self}: #{node.label} declares done but has no regions" if node.regions.empty?
        return unless node.done

        __check_reachable!(node, node.done, "#{node.label}'s done is")
        __check_buildable!(node.done, nil)
      end

      def __check_partition_keys!(klass, role)
        names = __attribute_names(klass)
        missing = partition_keys - names
        return if missing.empty?

        raise DefinitionError, "#{self}: #{role} #{klass} must declare #{missing.join(', ')}"
      end

      # A checkpoint the machine builds itself (from +source+'s payload, or from
      # the partition keys alone when +source+ is nil) must accept every attribute
      # it isn't given being missing.
      def __check_buildable!(checkpoint, source)
        return unless checkpoint.const_defined?(:Payload, false)

        supplied = (source ? __attribute_names(source) : []) + partition_keys
        checkpoint::Payload._schema._schema.each do |key, type|
          next if supplied.include?(key.to_sym)
          next if key.optional? || type.resolve(Plumb::Undefined).valid?

          raise DefinitionError,
                if source
                  "#{self}: #{source} => #{checkpoint} has no block, " \
                    "and #{checkpoint} requires #{key.to_sym}, which #{source} doesn't declare"
                else
                  "#{self}: done #{checkpoint} is appended with the partition keys only, " \
                    "and requires #{key.to_sym}"
                end
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
            # A checkpoint appended elsewhere can complete a join: decided here, under the guard.
            joins = checkpoint?(msg.class) ? instance.__decide_joins : EMPTY_ARRAY
            actions = Actions.build_for(joins, guard:, source: msg)
            actions += Actions.build_for(instance.__react(msg), source: msg)
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
      @runtime = Runtime.new(self.class.root)
      @uncommitted_events = nil
    end

    # Read-only view of the runtime state: the current configuration, the
    # events that entered it, and whether it's final.
    # @return [View]
    def machine = View.new(@runtime)

    # Evolve messages in log order: checkpoints move the machine, then any
    # +evolve+ handler updates the domain state.
    def evolve(messages)
      Array(messages).each do |msg|
        node = self.class.checkpoints[msg.class]
        @runtime.enter(msg, node) if node
        super([msg])
      end
      state
    end

    # Whether an active checkpoint accepts +command+. A command dispatched by an
    # +after+ block is refused once the machine has left the checkpoint entry
    # that started it, even if an active checkpoint accepts it.
    #
    # {Sourced.handle!} raises {Sourced::CommandNotHandledError} when it doesn't,
    # appending nothing; in background workers the command is acked with no events.
    #
    # @param command [Sourced::Message]
    # @return [Boolean]
    def handles_command?(command)
      return false if stale_timer?(command)

      handlers_for(command).any?
    end

    # Decide +command+ against the current configuration and state.
    #
    # The deepest active checkpoints that accept the command handle it: a
    # checkpoint's handler runs only when none of its active regions accepts the
    # command. Handlers in sibling regions run in definition order, each seeing
    # the events of the ones before it; one whose checkpoint an earlier handler
    # left doesn't run. Each handler emits exactly one of its declared
    # checkpoints (or none, when +nil+ is declared); a single declared one is
    # appended when the handler doesn't emit it. Joins the decision completes
    # are appended after it.
    #
    # @param command [Sourced::Message]
    # @return [Array<Sourced::Message>] the events produced
    # @raise [CheckpointError] if a handler emitted other checkpoints
    def decide(command)
      handlers = handlers_for(command)
      return [] if handlers.empty?

      deciding do
        handlers.each do |node|
          next unless @runtime.active_node?(node)

          definition = node.commands.fetch(command.class)
          started = @uncommitted_events.size
          instance_exec(state, command, &definition.block) if definition.block
          emitted = @uncommitted_events.drop(started).select { |evt| self.class.checkpoint?(evt.class) }
          emitted << event(definition.targets.first, command.payload&.to_h || {}) if emitted.empty? && definition.auto?
          check_checkpoints!(definition, emitted)
        end
        append_joins
      end
    end

    # Joins the current configuration completes: for each active checkpoint
    # whose regions are all final, its +done+ checkpoint, until none is left.
    # @api private
    # @return [Array<Sourced::Message>]
    def __decide_joins = deciding { append_joins }

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

    # Run the exit actions of the checkpoints +msg+ made the machine leave
    # (innermost first), the entry actions and timers of +msg+'s checkpoint,
    # then any +reaction+ to it. A checkpoint of an inactive region runs nothing.
    # @api private
    # @return [Array<Sourced::Message>] dispatched messages
    def __react(msg)
      __handling_reactions([msg]) do |message|
        if self.class.checkpoint?(message.class) && !@runtime.ignored?(message)
          @runtime.exited_by(message).each do |left|
            left.exits.each { |blk| instance_exec(state, message, &blk) }
          end
          entered = self.class.checkpoints.fetch(message.class)
          entered.entries.each { |blk| instance_exec(state, message, &blk) }
          start_timers(entered, message)
        end

        method_name = Sourced.message_method_name(React::PREFIX, message.class.to_s)
        respond_to?(method_name) ? Array(send(method_name, state, message)).compact : EMPTY_ARRAY
      end
    end

    private

    def deciding
      @uncommitted_events = []
      yield
      @uncommitted_events
    ensure
      @uncommitted_events = nil
    end

    # The deepest active checkpoints accepting +command+, in definition order.
    def handlers_for(command)
      accepting = @runtime.active_nodes.select { |node| node.commands.key?(command.class) }
      accepting.reject { |node| accepting.any? { |other| other.descendant_of?(node) } }
    end

    def append_joins
      MAX_JOINS.times do
        node = @runtime.active_nodes.find do |n|
          n.done && n.regions.all? { |region| @runtime.current_node(region).final? }
        end
        return unless node

        event(node.done)
      end
      raise Error, "#{self.class}: more than #{MAX_JOINS} joins in one decision"
    end

    # Timers for a checkpoint entry the machine has already left would only be
    # stale when delivered, so they aren't started.
    def start_timers(node, checkpoint)
      return unless @runtime.active_entry?(checkpoint.id)

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

      !@runtime.active_entry?(started_by)
    end

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
