# frozen_string_literal: true

require 'fugit'

module Sourced
  # Reactor base class for state machines: Sourced's take on statecharts.
  #
  # A machine's position is its *checkpoints*: handles declared with
  # {.Checkpoint} and placed with {.checkpoint}, {.final} or {.start}. Each
  # checkpoint lists the commands it accepts, and the checkpoints each command
  # may move the machine to. A checkpoint is also an event class: every move
  # appends one event of the target checkpoint's class
  # (+<type_prefix>.runtime.<name>+), whose payload says which checkpoints it
  # left and entered; the machine's position is replayed from those. Domain
  # facts are data events, which only change the domain +state+:
  #
  #   class OrderMachine < Sourced::Machine
  #     partition_by :order_id
  #
  #     Placed    = Checkpoint(:placed)
  #     Completed = Checkpoint(:completed)
  #     Cancelled = Checkpoint(:cancelled)
  #
  #     state { OrderState.new }
  #     evolve(ItemAdded) { |order, evt| order.add(evt.payload.sku) }
  #
  #     initial do
  #       command PlaceOrder => Placed
  #     end
  #
  #     checkpoint Placed do
  #       command CancelOrder => Cancelled
  #
  #       region :payment do                         # regions are active while Placed is
  #         initial do
  #           command Pay => [:captured, :declined] do |order, cmd|   # Symbols: an Array, not |
  #             event PaymentAttempted, ok: cmd.payload.ok   # data
  #             to(cmd.payload.ok ? :captured : :declined)
  #           end
  #         end
  #         final :captured
  #         checkpoint :declined
  #       end
  #
  #       region :shipping do
  #         initial { command Ship => :shipped }
  #         final :shipped
  #       end
  #
  #       done Completed                             # once every region is final
  #     end
  #
  #     final Completed
  #     final Cancelled
  #   end
  #
  # A command moves the machine to one of its declared checkpoints, or nowhere
  # when the declaration includes +nil+ (or has no arrow). Commands no active
  # checkpoint accepts are refused: see {#handles_command?}.
  #
  # Like a {Decider}, a machine is one reactor and one consumer group: every
  # message for a partition is decided one at a time, under one consistency
  # guard, so regions never run concurrently and share the domain state. Entry
  # and exit actions are reactions: they run when the machine claims the
  # transition, after the deciding command has committed.
  class Machine
    include Sourced::Evolve
    include Sourced::React
    include Sourced::Sync
    extend Sourced::Consumer

    # The machine's transitions, its children's starts and finishes, and the
    # evolve types of the machines mounted in it are evolved too, alongside the
    # projection's evolve types. They're the machine's own, not its
    # projection's (which has no handlers for them), so adopting a projection
    # with +evolve SomeProjection+ keeps them. Prepended after Sourced::Evolve's
    # class methods, so +super+ is the projection's list.
    module EvolveTypes
      # Fixed by {Machine.validate!}; computed on each call before it.
      def handled_messages_for_evolve
        return @__evolve_types if @__evolve_types

        tree = self.tree
        mounted = tree.mounted_machines.flat_map { |machine| machine.projection_class.handled_messages }
        (super + __transition_classes + tree.child_events.keys + mounted + handled_events).uniq
      end
    end
    singleton_class.prepend EvolveTypes

    Error = Class.new(Sourced::Error)

    # Raised when a machine's definition is invalid. Checked when the machine is
    # registered with a Router, and before its first instance is built.
    DefinitionError = Class.new(Error)

    # Raised when a handler moves the machine somewhere its command didn't declare.
    TransitionError = Class.new(Error)

    # Raised when a machine builds an event that isn't valid.
    InvalidEventError = Class.new(Error)

    INITIAL = :initial

    # Metadata keys on messages dispatched by an +after+ block: the id of the
    # transition that entered the checkpoint and started the timer, and the
    # checkpoint's path. One transition can enter several checkpoints, so a
    # timer belongs to the pair.
    ENTERED_BY = :entered_by
    ENTERED_AT = :entered_at

    # Joins chain (a join's checkpoint can complete an outer join), but a
    # definition can't make them chain forever. A safety net only.
    MAX_JOINS = 100

    # The base of the events {.on_stop} generates (+Started+, +Finished+).
    ChildEvent = Class.new(Sourced::Event)

    # The constants {.on_stop} generates.
    CHILD_MESSAGES = %i[Stop Started Finished].freeze

    NONE = [].freeze

    # A checkpoint: a position in a machine's chart, and the event recording
    # a transition to it. {Machine.Checkpoint} generates one subclass per name,
    # the checkpoint's *handle*: the tree places it (once per placement of its
    # machine, so a machine mounted twice has each of its checkpoints at two
    # paths), transitions target it, and the machine appends it when it runs
    # as a top machine. A machine that mounts another generates its own class
    # for each of the mounted machine's handles, with its own partition keys
    # and type (see {Machine.checkpoint_class}). Positions are the paths in
    # the payload, never the class.
    class Checkpoint < Sourced::Event
      # @return [Class<Checkpoint>] the handle this class records: itself, for a handle
      def self.checkpoint = @checkpoint

      # @return [Class<Machine>] the machine that appends this class
      def self.owner = @owner

      # @return [Class<Machine>] the machine that declares the handle
      def self.machine = checkpoint.owner

      # @return [Symbol] unique within {.machine}
      def self.checkpoint_name = @checkpoint_name

      # @param other [Class<Checkpoint>, History, Targets, nil]
      # @return [Targets]
      def self.|(other) = Targets.new(members: [self]) | other

      # A target re-entering this checkpoint where its regions were when it
      # was last left: each region's checkpoint (+deep+: the whole
      # configuration below it), or its default entry where it has none.
      # @return [History]
      def self.history(deep: false) = History.new(checkpoint:, deep:)

      def self.to_s = name || (checkpoint ? "#{machine}:#{checkpoint_name}" : super)
      def self.inspect = to_s

      # Generate a checkpoint class: +owner+'s partition keys and the transition
      # attributes. A type can be taken over only by a class for the same
      # machine (a reloaded or redefined one); any other clash raises, since
      # the registry would silently replace it.
      # @api private
      def self.__generate(type, owner:, checkpoint_name:, checkpoint: nil)
        existing = Sourced::Message.registry[type]
        unless existing.nil? || (Class === existing && existing < Checkpoint && existing.owner.group_id == owner.group_id)
          raise DefinitionError, "#{owner}: message type #{type.inspect} is already #{existing}'s"
        end

        keys = owner.partition_keys
        klass = define(type) do
          keys.each { |k| attribute k, String }
          Machine.__transition_attributes(self)
        end
        klass.instance_variable_set(:@owner, owner)
        klass.instance_variable_set(:@checkpoint, checkpoint || klass)
        klass.instance_variable_set(:@checkpoint_name, checkpoint_name)
        klass
      end
    end

    # @return [Boolean] whether +value+ is a checkpoint handle (not a class a mounting machine generated for one)
    def self.handle?(value) = Class === value && value < Checkpoint && value.checkpoint.equal?(value)

    # The checkpoints a transition may go to; +nil+ means it may stay.
    Targets = Data.define(:members) do
      # @param other [Checkpoint, History, Targets, nil]
      # @return [Targets]
      def |(other)
        case other
        in Targets then with(members: members | other.members)
        in nil | History then with(members: members | [other])
        in Class if Machine.handle?(other) then with(members: members | [other])
        end
      end
    end

    # A history pseudo-target ({Checkpoint.history}): +checkpoint+, entered
    # where its regions were when it was last left.
    History = Data.define(:checkpoint, :deep) do
      def |(other) = Targets.new(members: [self]) | other
      def to_s = deep ? "#{checkpoint}.history(deep: true)" : "#{checkpoint}.history"
      def inspect = to_s
    end

    # A command a checkpoint accepts, and the checkpoints it may move to.
    # +nil+ among the targets means "may stay".
    CommandDef = Data.define(:command_class, :targets, :block) do
      # Moves to its single target when the handler doesn't choose one.
      def auto? = targets.size == 1 && !targets.first.nil?
      def optional? = targets.include?(nil)
      def checkpoint_targets = targets.compact
    end

    # What a guard is called with: the domain state.
    GuardInterface = Types::Interface[:call]

    # The guard of an unguarded +always+ or +done+.
    UNGUARDED = ->(_state) { true }

    # A transition taken without a command (+always+, +done+), when its guard
    # passes. +guard+ is a +#call(state)+ (+UNGUARDED+ when declared without
    # one); +guard_label+ names it for rendering.
    Eventless = Data.define(:target, :guard, :guard_label) do
      # @param guard [Symbol, #call(state), nil] a Symbol is sent to the domain state
      # @return [Eventless]
      def self.build(target, guard)
        case guard
        in nil then new(target:, guard: UNGUARDED, guard_label: nil)
        in Symbol then new(target:, guard: ->(state) { state.public_send(guard) }, guard_label: guard.to_s)
        in GuardInterface then new(target:, guard:, guard_label: label_for(guard))
        end
      end

      # A callable's class or module name, or +'guard'+ for a lambda or an anonymous one.
      def self.label_for(guard)
        case guard
        in Proc then 'guard'
        in Module then guard.name || 'guard'
        else guard.class.name || 'guard'
        end
      end

      def guarded? = !guard.equal?(UNGUARDED)
      def pass?(state) = guard.call(state)
    end

    # A delayed action: runs when its checkpoint is entered, and future-dates
    # what it dispatches by +delay+ from the entering transition. +delay+ is as
    # declared: seconds, or a duration string; +duration+ is the parsed string.
    After = Data.define(:delay, :duration, :block) do
      # @param time [Time] the entering transition's +created_at+
      # @return [Time] when what the block dispatches is delivered
      def from(time) = duration ? duration.add_to_time(time).to_local_time : time + delay
    end

    # A child machine the parent tracks, declared with {.spawns} or {NodeBuilder#invoke}.
    # +node+, +keys+ and +on_done+ are for invoked children; +stop_on+ for spawned ones.
    ChildSpec = Data.define(:as, :machine, :kind, :stop_on, :node, :keys, :on_done) do
      def invoke? = kind == :invoke
    end

    # A child as its parent knows it: its own keys (beyond the parent's), the
    # checkpoint it started at, or the final one it finished at, and for an
    # invoked child, the parent checkpoint entry it was invoked for.
    ChildRef = Data.define(:keys, :state, :final, :entry_id) do
      def final? = final
    end

    # A join over children: once every child spawned as +as+ is final.
    ChildrenJoin = Data.define(:as, :definition)

    # A region whose checkpoints are another machine's, declared with
    # +region :name, SomeMachine+ or, for a compound checkpoint,
    # +checkpoint Handle, SomeMachine+. Its definition isn't copied: the tree
    # places the machine's root region under the checkpoint (see {Tree}).
    # +on_done+ maps the machine's root final checkpoints onto the parent's. A
    # compound region starts at the machine's +start+ checkpoint; any other at
    # its initial position.
    RegionMount = Data.define(:name, :machine, :on_done, :compound) do
      def compound? = compound
    end

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

      def label = root? ? 'the root region' : "region #{name.inspect}"
    end

    # A checkpoint (or a region's initial position) and what it accepts.
    class Node
      attr_reader :key, :region, :commands, :ons, :entries, :exits, :afters, :regions, :invokes, :children_joins, :always, :dones

      # @param key [Checkpoint, Symbol] the checkpoint, or {INITIAL}
      # @param final [Boolean]
      # @param region [Region] the region the checkpoint belongs to
      def initialize(key, final:, region:)
        @key = key
        @final = final
        @region = region
        @commands = {}
        @ons = {}
        @entries = []
        @exits = []
        @afters = []
        @regions = []
        @invokes = []
        @children_joins = []
        @always = []
        @dones = []
      end

      def final? = @final
      def initial? = key == INITIAL
      # Left as soon as it's entered: it has an unguarded +always+.
      def transient? = always.any? { |eventless| !eventless.guarded? }
      def label = initial? ? "#{region.label}'s initial position" : key.to_s
    end

    # Evaluates a region's block: +initial+, +checkpoint+, +final+.
    class RegionBuilder
      def initialize(machine, region)
        @machine = machine
        @region = region
      end

      def initial(&block) = @machine.send(:__define_node, @region, INITIAL, final: false, &block)

      def checkpoint(handle, machine = nil, on_done: {}, &block)
        @machine.send(:__define_checkpoint, @region, handle, machine, on_done, &block)
      end

      def final(handle, &block) = @machine.send(:__define_node, @region, @machine.send(:__handle, handle), final: true, &block)

      # @see Machine.Checkpoint
      def Checkpoint(name) = @machine.Checkpoint(name)
    end

    # Evaluates a checkpoint's block: +command+, +entry+, +exit+, +after+, +region+, +done+.
    class NodeBuilder
      def initialize(machine, node)
        @machine = machine
        @node = node
      end

      # @see Machine.Checkpoint
      def Checkpoint(name) = @machine.Checkpoint(name)

      # @overload command(command_class => target, &block)
      #   @param target [Checkpoint, Symbol, Targets, Array, nil] a checkpoint, a union of them, or +nil+
      # @overload command(command_class, &block)
      #   Data only: same as +command_class => nil+.
      def command(declaration, &block)
        definition = @machine.send(:__command_definition, declaration, block)
        if @node.commands.key?(definition.command_class)
          raise DefinitionError, "#{@machine}: #{@node.label} accepts #{definition.command_class} twice"
        end

        @node.commands[definition.command_class] = definition
      end

      # Move when the machine claims +event_class+, appended by another reactor,
      # while this checkpoint is active. Decided like a command: the same
      # targets, +to+ and +nil+; the block gets +(state, event)+. Skipped when
      # replaying.
      #
      # @overload on(event_class => target, &block)
      # @overload on(event_class, &block) data only
      def on(declaration, &block)
        definition = @machine.send(:__command_definition, declaration, block, what: 'on')
        event_class = definition.command_class
        case event_class
        in Class if event_class < Checkpoint
          raise DefinitionError, "#{@machine}: on #{event_class}: a checkpoint class; on takes another reactor's event"
        in Class if event_class < ChildEvent
          raise DefinitionError, "#{@machine}: on #{event_class}: a child machine's event; use on_done or children_done"
        in Class if event_class < Sourced::Event
          nil
        else
          raise DefinitionError, "#{@machine}: on takes an event, got #{event_class}"
        end
        raise DefinitionError, "#{@machine}: #{@node.label} takes #{event_class} twice" if @node.ons.key?(event_class)

        @node.ons[event_class] = definition
      end

      # Runs when the machine enters this checkpoint, as a reaction.
      # @yield [state, transition]
      def entry(&block)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so no entry actions" if @node.initial?

        @node.entries << block
      end

      # Runs when the machine leaves this checkpoint, as a reaction to the
      # transition that left it.
      # @yield [state, transition]
      def exit(&block) = @node.exits << block

      # Runs when the machine enters this checkpoint, as a reaction. Messages it
      # dispatches are delivered +delay+ after the entering transition, and
      # carry its id: a command delivered after the machine has left that
      # checkpoint entry is stale, and refused (see {Machine#handles_command?}).
      #
      #   checkpoint AwaitingPayment do
      #     after(3600) { |_state, _transition| dispatch ExpirePayment }
      #     command ExpirePayment => Expired
      #   end
      #
      # The delay is seconds, or a duration string as {Sourced::Message#at}
      # takes it (+'30m'+, +'1h30m'+, +'P1D'+).
      #
      # @param delay [Numeric, String]
      # @yield [state, transition]
      def after(delay, &block)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so no timers" if @node.initial?

        duration = Fugit.parse_duration(delay) if delay.is_a?(String)
        unless delay.is_a?(Numeric) || duration
          raise DefinitionError, "#{@machine}: after needs seconds or a duration string (ex. '30m', 'PT1H'), got #{delay.inspect}"
        end
        raise DefinitionError, "#{@machine}: after(#{delay.inspect}) needs a block" unless block

        @node.afters << After.new(delay:, duration:, block:)
      end

      # Declare a region, active while this checkpoint is. A checkpoint with
      # regions is parallel: all of its regions are active at once.
      #
      # With a block, the region's checkpoints are declared inline. With a
      # machine, they're that machine's: it runs as this region, in this
      # machine's partition, decided with it, its state this machine's (its
      # +evolve+ blocks apply to it too). It escalates through its root final
      # checkpoints, which +on_done+ maps onto ours.
      #
      #   region :payment, PaymentMachine, on_done: { PaymentMachine::Expired => Cancelled }
      #
      # @param name [Symbol] unique within the machine
      # @param machine [Class<Machine>, nil]
      # @param on_done [Hash{Checkpoint => Checkpoint}] the machine's root final => our checkpoint
      def region(name, machine = nil, on_done: {}, &block)
        raise DefinitionError, "#{@machine}: the initial position can't have regions" if @node.initial?
        raise DefinitionError, "#{@machine}: final checkpoint #{@node.key} can't have regions" if @node.final?
        if @node.regions.any? { |r| r.is_a?(RegionMount) && r.compound? }
          raise DefinitionError, "#{@machine}: #{@node.key} is a machine's compound checkpoint, so it can't have regions"
        end

        if machine
          raise DefinitionError, "#{@machine}: region #{name.inspect} takes a machine or a block, not both" if block

          @machine.send(:__mount_region, @node, name.to_sym, machine, on_done)
        else
          raise DefinitionError, "#{@machine}: region #{name.inspect}: on_done is for a mounted machine" if on_done.any?

          @machine.send(:__define_region, @node, name.to_sym, &block)
        end
      end

      # Join: once every region of this checkpoint is at a final checkpoint,
      # the machine moves to +target+, in the same decision. Several guarded
      # joins are tried in order, the first passing one taken; with none
      # passing (and no unguarded one), the machine stays until a later
      # decision passes one.
      #
      #   done Delivered, if: :all_paid?
      #   done Partial
      #
      # @param target [Checkpoint, Symbol] a checkpoint of this region or an ancestor's
      # @param guard [Symbol, #call, nil] +if:+, of the domain state
      def done(target, if: nil)
        @node.dones << @machine.send(:__eventless, @node, 'done', target, binding.local_variable_get(:if), @node.dones)
      end

      # An eventless transition: as soon as this checkpoint is entered, and
      # after every decision while it's active, the first +always+ whose guard
      # passes moves the machine, in the same decision. A checkpoint with an
      # unguarded +always+ is transient: it can't accept commands or have
      # timers.
      #
      #   checkpoint RollingUp do
      #     always Voided, if: ->(run) { run.any_item?(:void) }
      #     always Paid, if: :all_items_paid?
      #     always Settling
      #   end
      #
      # @param target [Checkpoint, Symbol]
      # @param guard [Symbol, #call, nil] +if:+, of the domain state
      def always(target, if: nil)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so no always" if @node.initial?
        raise DefinitionError, "#{@machine}: final checkpoint #{@node.label} can't have always" if @node.final?

        @node.always << @machine.send(:__eventless, @node, 'always', target, binding.local_variable_get(:if), @node.always)
      end

      # Run a child machine while this checkpoint is active: started in the
      # decision that enters the checkpoint, sent its +Stop+ when the checkpoint
      # is left, and its final checkpoint mapped onto one of ours.
      #
      #   checkpoint AwaitingPayment do
      #     invoke PaymentMachine, as: :payment,
      #            keys: ->(_order) { { payment_id: SecureRandom.uuid } },
      #            on_done: { PaymentMachine::Captured => Paid, PaymentMachine::Declined => PaymentFailed }
      #   end
      #
      # @param child [Class<Machine>] a machine with +start+ and +on_stop+
      # @param as [Symbol] name for {View#children}
      # @param keys [#call(state)] the child's own keys, and any input for its +Started+
      # @param on_done [Hash{Checkpoint => Checkpoint}] child final checkpoint => our checkpoint
      def invoke(child, as:, keys:, on_done: {})
        raise DefinitionError, "#{@machine}: #{@node.label} can't invoke" if @node.initial? || @node.final?

        @machine.send(:__define_child, child, as:, kind: :invoke, node: @node, keys:, on_done:)
      end

      # Join over spawned children: once every child spawned as +as+ is at a
      # final checkpoint (and at least one was spawned), the machine moves to
      # the declared checkpoint, as a command handler would.
      #
      #   children_done :lines => LinesDone | LinesPartiallyFailed do |order|
      #     to(machine.children(:lines).all? { |line| line.state == LineMachine::Picked } ? LinesDone : LinesPartiallyFailed)
      #   end
      def children_done(declaration, &block)
        unless declaration.is_a?(Hash) && declaration.size == 1
          raise DefinitionError, "#{@machine}: children_done takes { as => checkpoint }, got #{declaration.inspect}"
        end

        as, target = declaration.first
        targets = @machine.send(:__flatten_targets, target)
        raise DefinitionError, "#{@machine}: children_done #{as.inspect} must move to a checkpoint, not nil" if targets.include?(nil)
        if block.nil? && targets.size > 1
          raise DefinitionError, "#{@machine}: children_done #{as.inspect} with several checkpoints needs a block"
        end

        definition = CommandDef.new(command_class: "children_done #{as.inspect}", targets: targets.uniq, block:)
        @node.children_joins << ChildrenJoin.new(as:, definition:)
      end
    end

    # Read-only view of a machine's runtime state, available as {#machine}.
    class View
      def initialize(runtime) = @runtime = runtime

      # The current configuration: a checkpoint (or +:initial+), or for a
      # checkpoint with regions, +{ checkpoint => { region_name => value, … } }+,
      # and for a compound one, +{ checkpoint => value }+.
      def value = @runtime.value

      # @overload in?(checkpoint)
      #   Whether +checkpoint+ is active, in any region.
      # @overload in?(path)
      #   Whether the checkpoint at +path+ (+'pending.submission.check.submitting'+) is active.
      # @overload in?(region_name, checkpoint)
      #   Whether a region named +region_name+ is active and at +checkpoint+ (or +:initial+).
      def in?(*args)
        case args
        in [Class => checkpoint]
          @runtime.active_nodes.any? { |node| node.key == checkpoint }
        in [String => path]
          @runtime.active_nodes.any? { |node| node.path == path }
        in [Symbol => region_name, checkpoint]
          @runtime.active_regions.any? { |r| r.name == region_name && @runtime.current_node(r).key == checkpoint }
        end
      end

      # Whether the root region is at a final checkpoint.
      def final? = @runtime.current_node(@runtime.root).final?

      # @overload children(as)
      #   @return [Array<ChildRef>] the children spawned or invoked as +as+
      # @overload children
      #   @return [Hash{Symbol => Array<ChildRef>}] every child, by +as+
      def children(as = nil) = as ? @runtime.children(as) : @runtime.all_children

      # The checkpoint the root region, or the active region named
      # +region_name+, is at: a handle, or +:initial+; nil when no region of
      # that name is active.
      def current(region_name = nil)
        region = region_name ? @runtime.active_regions.find { |r| r.name == region_name } : @runtime.root
        region && @runtime.current_node(region).key
      end

      # The transition that entered the current checkpoint of the root region,
      # or of the active region named +region_name+ (nil while it's at its
      # initial position or inactive).
      def entered_by(region_name = nil)
        region = region_name ? @runtime.active_regions.find { |r| r.name == region_name } : @runtime.root
        region && @runtime.entered_by(region)
      end
    end

    # A loaded machine, as returned by {.load}. +value+ is the current
    # configuration (see {View#value}), +children+ its children by name, and
    # +state+ the domain state.
    Snapshot = Data.define(:value, :state, :children)

    # The machine's own bookkeeping, separate from the domain state: per
    # region, the current checkpoint, the transition that entered it and the
    # checkpoints entered since the region was; and the children.
    class Runtime
      Position = Struct.new(:current, :entered_by, :visited)

      attr_reader :root

      # @param tree [Tree]
      def initialize(tree)
        @tree = tree
        @root = tree.root
        @positions = {}
        @history = {}
        @children = {}
      end

      # Record a child's start or finish. An invoked child keeps the entry it
      # was first invoked for.
      def track_child(spec, keys, state:, final:, entry_id:)
        refs = (@children[spec.as] ||= {})
        known = refs[keys]
        refs[keys] = ChildRef.new(keys:, state:, final:, entry_id: known ? known.entry_id : entry_id)
      end

      def children(as) = (@children[as] || {}).values
      def all_children = @children.transform_values(&:values)

      def current_node(region)
        key = position(region).current
        key == INITIAL ? region.initial_node : region.nodes.fetch(key)
      end

      def entered_by(region) = position(region).entered_by

      # Whether +node+ was entered since its region was last entered (or, for
      # the root region, ever). The current position counts as visited, and a
      # region's initial position is visited once it has moved on from it.
      def visited?(node)
        visited = position(node.region).visited
        node.initial? ? active_node?(node) || visited.any? : visited.include?(node.key)
      end

      def active?(region)
        region.root? || (active?(region.parent.region) && position(region.parent.region).current == region.parent.key)
      end

      def active_node?(node) = active?(node.region) && current_node(node.region).equal?(node)

      # @return [Array<PlacedNode>] the active checkpoints, outermost first, regions in definition order
      def active_nodes = active_nodes_in(root)

      # @return [Array<PlacedRegion>] the active regions, outermost first, in definition order
      def active_regions = active_nodes.flat_map(&:regions).unshift(root)

      # Whether +event_id+ entered a checkpoint that's still active.
      def active_entry?(event_id)
        active_regions.any? { |region| position(region).entered_by&.id == event_id }
      end

      # Whether +node+ is active, entered by +event_id+.
      def active_entry_of?(node, event_id) = active_node?(node) && entered_by(node.region)&.id == event_id

      def value = value_of(root)

      # What moving from +source+ to +target+ leaves and enters, given the
      # current configuration. The transition's domain is the innermost region
      # that holds both: +source+'s region or an ancestor's, or for a target
      # below +source+, the one of +source+'s regions it's in. Everything
      # active in the domain is left, innermost first; the path from the domain
      # down to +target+ is entered, outermost first, then +target+'s default
      # configuration: each compound checkpoint's start, all the way down.
      #
      # With +history+, +target+'s regions are entered where they were when
      # it was last left (counting what this transition leaves), instead.
      #
      # @param source [PlacedNode] the active checkpoint (or initial position) taking the transition
      # @param target [PlacedNode]
      # @param history [History, nil]
      # @return [Array(Array<PlacedNode>, Array<PlacedNode>)] exited and entered
      def transition_sets(source, target, history: nil)
        lineage = target.region.lineage
        domain = source.regions.find { |r| lineage.include?(r) } || source.region.lineage.find { |r| lineage.include?(r) }
        raise Error, "no region holds both #{source.label} and #{target.label}" unless domain

        exited = exits_from(current_node(domain)).reject(&:initial?)
        [exited, entry_path(target, domain, history:, last: history && remembered(exited))]
      end

      # Apply a transition, decided now or replayed: each entered checkpoint
      # becomes its region's current one, entered by +event+, and its regions
      # start afresh.
      #
      # @param event [Checkpoint] a checkpoint event (or a child's +Started+)
      # @raise [Error] if a path doesn't resolve, or enters an inactive region
      def apply(event)
        # A region's history is where it was when its checkpoint was last left.
        event.payload.exited.each { |path| @tree.node_at(path).regions.each { |region| remember(region) } }
        event.payload.entered.each do |path|
          node = @tree.node_at(path)
          raise Error, "#{event.type} #{event.id} enters #{path}, in an inactive region" unless active?(node.region)

          pos = position(node.region)
          pos.current = node.key
          pos.entered_by = event
          pos.visited << node.key unless pos.visited.include?(node.key)
          node.regions.each { |region| @positions[region] = Position.new(INITIAL, nil, []) }
        end
      end

      private

      def position(region) = @positions[region] ||= Position.new(INITIAL, nil, [])

      def active_nodes_in(region)
        node = current_node(region)
        [node, *node.regions.flat_map { |child| active_nodes_in(child) }]
      end

      # Leaving +node+ leaves its active descendants first.
      def exits_from(node)
        node.regions.flat_map { |region| exits_from(current_node(region)) } + [node]
      end

      # @param last [Hash{PlacedRegion => Symbol}, nil] each region's history, for +history+
      def entry_path(target, domain, history:, last:)
        chain = [target]
        chain.unshift(chain.first.region.parent) until chain.first.region.equal?(domain)
        chain.each_with_index.flat_map do |node, i|
          below = chain[i + 1]
          next [node] if below

          history ? restore(node, deep: history.deep, last:) : @tree.default_entry(node)
        end
      end

      def remember(region)
        key = position(region).current
        key == INITIAL ? @history.delete(region) : @history[region] = key
      end

      # Regions' history once +exited+ is left: what it leaves counts as last.
      def remembered(exited)
        leaving = exited.flat_map { |node| node.regions.map { |region| [region, position(region).current] } }
        @history.merge(leaving.to_h).reject { |_, key| key == INITIAL }
      end

      # +node+, then each region at its last checkpoint (deep: restored all the
      # way down), or its default entry where it has none.
      def restore(node, deep:, last:)
        [node, *node.regions.flat_map do |region|
          key = last[region]
          if key.nil?
            region.compound? ? @tree.default_entry(region.start_node) : NONE
          else
            child = region.nodes.fetch(key)
            deep ? restore(child, deep:, last:) : @tree.default_entry(child)
          end
        end]
      end

      def value_of(region)
        node = current_node(region)
        return node.key if node.regions.empty?

        compound = node.compound_region
        return { node.key => value_of(compound) } if compound

        { node.key => node.regions.to_h { |child| [child.name, value_of(child)] } }
      end
    end

    class << self
      # The root region: the machine's top-level checkpoints.
      # @return [Region]
      def root = @root ||= Region.new(:root, nil)

      # @return [Hash{Symbol => Checkpoint}] the checkpoints this machine declares, by name
      def declared_checkpoints = @declared_checkpoints ||= {}

      # @return [Hash{Checkpoint => Node}] the checkpoints this machine places, in any of its regions
      def nodes = @nodes ||= {}

      # @return [Hash{Symbol => Region}] every region but the root, declared inline
      def regions = @regions ||= {}

      # @return [Hash{Symbol => RegionMount}] regions that are other machines
      def mounts = @mounts ||= {}

      # @return [Hash{Symbol => ChildSpec}] child machines, by +as+
      def children_specs = @children_specs ||= {}

      # @param klass [Class]
      def child_event?(klass) = tree.child_event?(klass)

      # The checkpoint a parent starts this machine at, with +Started+.
      # @return [Checkpoint, nil]
      attr_reader :start_checkpoint

      # The final checkpoint this machine's +Stop+ moves it to.
      # @return [Checkpoint, nil]
      attr_reader :stop_checkpoint

      # Declare a checkpoint: a handle naming a position in this machine's
      # chart, and the event recording a transition to it, of type
      # +<type_prefix>.runtime.<name>+. Place it with {.checkpoint}, {.final}
      # or {.start}.
      #
      #   Placed = Checkpoint(:placed)
      #
      # Declare +partition_by+ and +consumer_group+ first: the class takes the
      # partition keys, and its type the prefix.
      #
      # @param name [Symbol]
      # @return [Class<Checkpoint>] the same handle for the same name
      def Checkpoint(name)
        name = name.to_sym
        raise DefinitionError, "#{self}: a checkpoint can't be named #{INITIAL.inspect}" if name == INITIAL
        if name.to_s.include?('.')
          raise DefinitionError, "#{self}: checkpoint names can't contain dots, got #{name.inspect}"
        end

        declared_checkpoints[name] ||= begin
          raise DefinitionError, 'a machine needs a name or a consumer_group before its checkpoints' unless group_id
          if @validated
            raise DefinitionError, "#{self}: checkpoint #{name.inspect} declared after the machine was validated"
          end

          Checkpoint.__generate("#{type_prefix}.runtime.#{name}", owner: self, checkpoint_name: name)
                    .tap { |klass| __transition_classes << klass }
        end
      end

      # The class this machine appends for +handle+: the handle itself for one
      # of its own checkpoints, or the class it generated for a mounted
      # machine's.
      #
      # @param handle [Class<Checkpoint>]
      # @return [Class<Checkpoint>]
      def checkpoint_class(handle)
        handle = handle.checkpoint
        return handle if handle.machine.equal?(self)

        __mounted_classes.fetch(handle) do
          raise Error, "#{self} doesn't mount #{handle.machine}, so it has no class for #{handle}"
        end
      end

      # Every checkpoint class this machine appends, and its +Started+ for a
      # child: what a projection subscribes to for every move.
      # @return [Array<Class>]
      def transition_classes = __transition_classes

      # Declare where a parent starts this machine (with +Started+), without a
      # command. With a machine, the start is a compound checkpoint, entered at
      # that machine's own start.
      # @param handle [Checkpoint, Symbol]
      def start(handle, machine = nil, on_done: {}, &block)
        raise DefinitionError, "#{self}: start is declared twice" if @start_checkpoint

        @start_checkpoint = __handle(handle)
        checkpoint(@start_checkpoint, machine, on_done:, &block)
      end

      # Declare what this machine becomes when its parent stops it. Generates a
      # +Stop+ command (+self::Stop+, keyed by this machine's partition keys),
      # accepted from any started, non-final position, that moves to +handle+,
      # a final checkpoint of the root region (placed here when it isn't
      # already). Also generates +Started+ and +Finished+, the events a parent
      # tracks its children by. Leaving that way runs exit actions as usual.
      # @param handle [Checkpoint, Symbol]
      def on_stop(handle)
        raise DefinitionError, "#{self}: on_stop is declared twice" if @stop_checkpoint
        raise DefinitionError, 'a machine with on_stop needs a name' unless name
        raise DefinitionError, "#{self}: declare partition_by before on_stop" if partition_keys.empty?
        if (taken = CHILD_MESSAGES.find { |constant| const_defined?(constant, false) })
          raise DefinitionError, "#{self}: on_stop generates #{taken}, which is already defined; name that constant differently"
        end

        @stop_checkpoint = __handle(handle)
        node = nodes[@stop_checkpoint] || final(@stop_checkpoint)
        unless node.final? && node.region.root?
          raise DefinitionError, "#{self}: on_stop #{@stop_checkpoint} must be a final checkpoint of the root region"
        end

        keys = partition_keys
        const_set(:Stop, Sourced::Command.define("#{type_prefix}.runtime.child.stop") { keys.each { |k| attribute k, String } })
        __define_child_messages
      end

      # Track a child machine: its +Started+ (appended by {Machine#spawn}) and
      # +Finished+ are evolved, for {View#children} and +children_done+.
      # Children are stopped, through their +Stop+, when this machine enters
      # one of the +stop_on+ checkpoints.
      #
      # @param child [Class<Machine>] a machine with +start+ and +on_stop+
      # @param as [Symbol]
      # @param stop_on [Checkpoint, Symbol, Array] checkpoints of this machine
      def spawns(child, as:, stop_on: [])
        __define_child(child, as:, kind: :spawn, stop_on: Array(stop_on).map { |handle| __handle(handle) })
      end

      # Before any checkpoint or mount: their classes take the keys.
      def partition_by(*keys)
        __check_declared_first!(:partition_by)
        super
      end

      # Before any checkpoint or mount: their types take the prefix.
      def consumer_group(id)
        __check_declared_first!(:consumer_group)
        super
      end

      # The machine's definition as a JSON-serializable Hash. See {Chart}.
      #
      # @return [Hash]
      def to_h
        validate!
        Chart.new(self).to_h
      end

      # The machine as a Mermaid state diagram. See {Mermaid}.
      #
      # @return [String]
      def to_mermaid
        validate!
        Mermaid.new(self).to_s
      end

      # Message type prefix derived from +group_id+ (the class name unless
      # +consumer_group+ fixes it): +Shop::OrderMachine+ => +shop.order_machine+.
      def type_prefix = Types::Underscore.parse(group_id.to_s.gsub('::', '.'))

      # The machine's position before its first checkpoint.
      def initial(&block) = __define_node(root, INITIAL, final: false, &block)

      # Place a checkpoint, and declare the commands it accepts.
      #
      # With a machine, the checkpoint is compound: its sub-states are that
      # machine's checkpoints, entered at its +start+:
      #
      #   checkpoint Unsettled, UnsettledMachine, on_done: { UnsettledMachine::Sent => Submitted }
      #
      # The block declares the compound checkpoint's own commands and actions.
      # The machine runs in this machine's partition and instance, its state
      # this machine's; it escalates through its root finals, which +on_done+
      # maps onto our checkpoints.
      #
      # @param handle [Checkpoint, Symbol] one of this machine's checkpoints; a Symbol declares it
      # @param machine [Class<Machine>, nil] a machine with +start+, for a compound checkpoint
      # @param on_done [Hash{Checkpoint => Checkpoint}] the machine's root final => our checkpoint
      def checkpoint(handle, machine = nil, on_done: {}, &block)
        __define_checkpoint(root, handle, machine, on_done, &block)
      end

      # Place a final checkpoint. It accepts no commands.
      # @param handle [Checkpoint, Symbol]
      def final(handle, &block) = __define_node(root, __handle(handle), final: true, &block)

      # The machine's definition placed into one tree (see {Tree}), which the
      # runtime, routing and rendering work on. Built once the machine is
      # validated; before that (while the class body runs), built on each call.
      # @return [Tree]
      def tree = @tree || Tree.new(self)

      # @return [Array<PlacedNode>] every node in the machine's tree, initial positions included
      def all_nodes = tree.all_nodes

      # Fixed by {.validate!}; computed on each call before it.
      # @return [Array<Class>] every command any checkpoint accepts, and +Stop+
      def handled_commands
        return @__handled_commands if @__handled_commands

        commands = all_nodes.flat_map { |node| node.commands.keys }
        commands << self::Stop if stop_checkpoint
        commands.uniq
      end

      # Fixed by {.validate!}; computed on each call before it.
      # @return [Array<Class>] every event an +on+ declares
      def handled_events
        return @__handled_events if @__handled_events

        all_nodes.flat_map { |node| node.ons.keys }.uniq
      end

      # Claimed: commands and +on+ events to decide, transitions to run entry
      # and exit actions for, children's starts and finishes (which joins
      # decide on), and any events with a +reaction+.
      def handled_messages
        (handled_commands + handled_events + __transition_classes + tree.child_events.keys + handled_messages_for_react).uniq
      end

      # Check the definition. Memoised; runs before the first instance is built,
      # and when the machine is registered with a Router.
      #
      # @raise [DefinitionError]
      # @return [true]
      def validate!
        # +@validating+: a child that spawns its parent back.
        return true if @validated || @validating

        @validating = true
        raise DefinitionError, "#{self} needs partition_by" if partition_keys.empty?
        raise DefinitionError, 'a machine needs a name or a consumer_group' unless group_id

        tree = Tree.new(self)
        __validate!(tree)
        # Children are started from here, so they must be valid too.
        children_specs.each_value { |spec| spec.machine.validate! }
        @tree = tree
        # What the runtime reads per message, fixed now that the definition is.
        @__handled_commands = handled_commands.freeze
        @__handled_events = handled_events.freeze
        @__evolve_types = handled_messages_for_evolve.freeze
        @__start_paths = __start_paths.freeze if start_checkpoint
        __transition_classes.freeze
        @validated = true
      ensure
        @validating = false
      end

      # Build executable actions for a claimed batch.
      #
      # +history+ comes from an unbounded read over the machine's evolve types,
      # so it holds the claimed transitions (and anything appended since), in
      # log order. The machine evolves all of it: commands are decided against
      # the latest state, and transitions run the exit and entry actions of the
      # checkpoints they left and entered.
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
        types = handled_messages_for_evolve
        evolvable = new_messages.select { |msg| types.include?(msg.class) }
        __process(partition_values, new_messages, history.messages + evolvable, guard: history.guard, replaying:)
      end

      # Load a machine instance and return its configuration and domain state.
      #
      # @param store [Sourced::Store, nil] defaults to +Sourced.store+
      # @return [Snapshot]
      def load(store: nil, **values)
        instance, = Sourced.load(self, store:, **values)
        Snapshot.new(value: instance.machine.value, state: instance.state, children: instance.machine.children)
      end

      # The events that move this machine: its checkpoint classes, and for a child, +Started+.
      # @api private
      def __transition_classes = @__transition_classes ||= []

      # @api private
      # @return [Hash{Class<Checkpoint> => Class<Checkpoint>}] mounted machines' handles => our classes for them
      def __mounted_classes = @__mounted_classes ||= {}

      # What a parent's +Started+ enters: this machine's start, and its default configuration.
      # @api private
      # @return [Array<String>]
      def __start_paths = @__start_paths || tree.default_entry_paths(start_checkpoint)

      # @api private
      # @return [Class, nil] +Finished+, for a machine with +on_stop+
      def __finished_class = @__finished_class

      private

      def __check_declared_first!(what)
        return if declared_checkpoints.empty? && mounts.empty?

        raise DefinitionError, "#{self}: declare #{what} before checkpoints and mounts"
      end

      # Generate our class for each checkpoint of +machine+, and of the machines
      # it mounts, all the way down: the events we append while it runs in our
      # partition. A machine without partition keys is never a top machine, so
      # it generates none; whichever machine mounts it does.
      def __generate_mounted(machine)
        return if partition_keys.empty?

        machine.declared_checkpoints.each_value do |handle|
          next if __mounted_classes.key?(handle)

          type = "#{type_prefix}.runtime.mounted.#{machine.type_prefix}.#{handle.checkpoint_name}"
          klass = Checkpoint.__generate(type, owner: self, checkpoint: handle, checkpoint_name: handle.checkpoint_name)
          __mounted_classes[handle] = klass
          __transition_classes << klass
          __name_mounted(klass, handle)
        end
        machine.mounts.each_value { |mount| __generate_mounted(mount.machine) }
      end

      # +Mounted::<mounted machine's name>::<handle's constant>+, under this machine.
      def __name_mounted(klass, handle)
        return unless name && handle.machine.name && handle.name

        namespace = ['Mounted', *handle.machine.name.split('::')].reduce(self) do |mod, segment|
          mod.const_defined?(segment, false) ? mod.const_get(segment, false) : mod.const_set(segment, Module.new)
        end
        constant = handle.name.split('::').last
        namespace.const_set(constant, klass) unless namespace.const_defined?(constant, false)
      end

      def __define_child_messages
        keys = partition_keys
        const_set(:Started, ChildEvent.define("#{type_prefix}.runtime.child.started") do
          keys.each { |k| attribute k, String }
          Machine.__transition_attributes(self)
          attribute :input, Sourced::Types::Hash.default { {} }
        end)
        __transition_classes << (@__started_class = self::Started)
        @__finished_class = const_set(:Finished, ChildEvent.define("#{type_prefix}.runtime.child.finished") do
          keys.each { |k| attribute k, String }
          attribute :final, String
        end)
      end

      def __define_checkpoint(region, handle, machine, on_done, &block)
        handle = __handle(handle)
        if machine.nil?
          raise DefinitionError, "#{self}: checkpoint #{handle}: on_done is for a compound checkpoint" if on_done.any?

          return __define_node(region, handle, final: false, &block)
        end
        unless machine.is_a?(Class) && machine < Machine
          raise DefinitionError, "#{self}: checkpoint #{handle}: #{machine.inspect} isn't a Sourced::Machine"
        end
        raise DefinitionError, "#{self}: checkpoint #{handle}: #{machine} has no start checkpoint" unless machine.start_checkpoint

        # Named after its checkpoint, so one machine can be several compound checkpoints.
        mount = __mount(handle.checkpoint_name, machine, on_done, compound: true)
        __define_node(region, handle, final: false, mount:, &block)
      end

      def __define_node(region, key, final:, mount: nil, &block)
        key = __handle(key) unless key == INITIAL
        if region.nodes.key?(key) || nodes.key?(key)
          raise DefinitionError, "#{self}: #{key == INITIAL ? "#{region.label}'s initial position" : key} is placed twice"
        end

        node = Node.new(key, final:, region:)
        region.nodes[key] = node
        nodes[key] = node unless key == INITIAL
        node.regions << mount if mount
        NodeBuilder.new(self, node).instance_eval(&block) if block
        node
      end

      def __define_region(node, name, &block)
        raise DefinitionError, "#{self}: region #{name.inspect} is declared twice" if regions.key?(name) || mounts.key?(name)

        region = Region.new(name, node)
        regions[name] = region
        node.regions << region
        RegionBuilder.new(self, region).instance_eval(&block) if block
        region
      end

      def __mount_region(node, name, machine, on_done)
        unless machine.is_a?(Class) && machine < Machine
          raise DefinitionError, "#{self}: region #{name.inspect}: #{machine.inspect} isn't a Sourced::Machine"
        end

        mount = __mount(name, machine, on_done, compound: false)
        node.regions << mount
        mount
      end

      # Record a mount of +machine+ under +name+, unique among this machine's regions.
      def __mount(name, machine, on_done, compound:)
        raise DefinitionError, "#{self}: region #{name.inspect} is declared twice" if regions.key?(name) || mounts.key?(name)
        raise DefinitionError, "#{self}: region #{name.inspect}: a machine can't be its own region" if machine <= self
        # Reactions are methods, and sync blocks the machine's own: neither can
        # run on this machine's instance. What every machine inherits is ours too.
        if (machine.handled_messages_for_react - Machine.handled_messages_for_react).any? ||
           (machine.sync_blocks - Machine.sync_blocks).any? ||
           (machine.after_sync_blocks - Machine.after_sync_blocks).any?
          raise DefinitionError,
                "#{self}: region #{name.inspect}: #{machine} declares reactions or sync blocks, " \
                "which it can't run as a region; declare them on #{self}"
        end

        on_done = on_done.to_h { |final, target| [machine.send(:__handle, final, declare: false), __own_target(target)] }
        mounts[name] = RegionMount.new(name:, machine:, on_done:, compound:).tap { __generate_mounted(machine) }
      end

      def __define_child(child, as:, kind:, node: nil, keys: nil, on_done: {}, stop_on: [])
        raise DefinitionError, "#{self}: #{child.inspect} isn't a Sourced::Machine" unless child.is_a?(Class) && child < Machine
        raise DefinitionError, "#{self}: child #{as.inspect} is declared twice" if children_specs.key?(as)
        raise DefinitionError, "#{self}: #{child} is already a child as another name" if children_specs.values.any? { |c| c.machine == child }
        raise DefinitionError, "#{self}: #{child} has no start checkpoint" unless child.start_checkpoint
        raise DefinitionError, "#{self}: #{child} has no on_stop" unless child.stop_checkpoint
        raise DefinitionError, "#{self}: invoke #{child} needs keys: ->(state) { … }" if kind == :invoke && !keys.respond_to?(:call)

        on_done = on_done.to_h { |final, target| [child.send(:__handle, final, declare: false), __own_target(target)] }
        spec = ChildSpec.new(as:, machine: child, kind:, stop_on:, node:, keys:, on_done:)
        children_specs[as] = spec
        node.invokes << spec if node
        spec
      end

      # An +always+ or +done+ of +node+, after +declared+ (its siblings so far).
      def __eventless(node, kind, target, guard, declared)
        unless guard in nil | Symbol | GuardInterface
          raise DefinitionError, "#{self}: #{node.label}'s #{kind}: if: takes a Symbol or a #call(state), got #{guard.inspect}"
        end

        eventless = Eventless.build(kind == 'done' ? __own_target(target) : __target(target), guard)
        if declared.any? { |other| !other.guarded? }
          raise DefinitionError, "#{self}: #{node.label}'s #{kind} #{eventless.target} is unreachable, after an unguarded #{kind}"
        end

        eventless
      end

      # One of this machine's checkpoints. A Symbol names one, declaring it
      # unless +declare+ is false.
      def __handle(target, declare: true)
        case target
        in Class if Machine.handle?(target) && target.machine == self then target
        in Class if Machine.handle?(target)
          raise DefinitionError, "#{self}: #{target} is #{target.machine}'s checkpoint, not #{self}'s"
        in Symbol | String if declare then __name_declared(Checkpoint(target))
        in Symbol | String
          declared_checkpoints.fetch(target.to_sym) { raise DefinitionError, "#{self} has no checkpoint #{target.inspect}" }
        else
          raise DefinitionError, "#{self}: checkpoints are Checkpoint handles or Symbols, got #{target.inspect}"
        end
      end

      # A checkpoint declared by Symbol has no constant of its own: set one
      # (+:awaiting_payment+ => +AwaitingPayment+), unless that name is taken.
      def __name_declared(handle)
        return handle if handle.name

        constant = handle.checkpoint_name.to_s.split('_').map(&:capitalize).join.to_sym
        # Left for on_stop's messages: such a handle stays anonymous.
        return handle if CHILD_MESSAGES.include?(constant)

        const_set(constant, handle) unless const_defined?(constant, false)
        handle
      end

      # @param what [String] the macro declaring it, for errors: +command+ or +on+
      def __command_definition(declaration, block, what: 'command')
        command_class, targets =
          case declaration
          in Hash if declaration.size == 1
            [declaration.keys.first, __flatten_targets(declaration.values.first)]
          in Class
            [declaration, [nil]]
          else
            raise DefinitionError, "#{self}: invalid #{what} declaration #{declaration.inspect}"
          end

        unless command_class in Class and command_class < Sourced::Message
          raise DefinitionError, "#{self}: #{what} takes a message class, got #{command_class.inspect}"
        end
        if block.nil? && (targets.size > 1 || targets == [nil])
          raise DefinitionError,
                "#{self}: #{command_class} => #{targets.map { |t| t || 'nil' }.join(' | ')} " \
                'needs a block to choose where to go'
        end

        CommandDef.new(command_class:, targets: targets.uniq, block:)
      end

      # @return [Array<Checkpoint, nil>]
      def __flatten_targets(target)
        case target
        in nil then [nil]
        in Targets then target.members.map { |member| member && __target(member) }
        in Array then target.flat_map { |member| __flatten_targets(member) }
        else [__target(target)]
        end
      end

      # Where a transition may go: one of our checkpoints, or one of a mounted
      # machine's, below the checkpoint that declares it (checked on validation),
      # or a {History} of one.
      def __target(target, declare: true)
        case target
        in History then target
        in Class if Machine.handle?(target) then target
        else __handle(target, declare:)
        end
      end

      # One of our checkpoints, or a {History} of one.
      def __own_target(target) = (target in History) ? target : __handle(target)

      # @param node [PlacedNode]
      def __check_target!(tree, node, target)
        handle = target.checkpoint
        placed = tree.resolve(node, target)
        return if handle.machine == node.machine || placed.descendant_of?(node)

        raise DefinitionError,
              "#{self}: #{node.label} targets #{handle}, another machine's checkpoint that isn't below it; " \
              'a mounted machine reaches ours through on_done'
      end

      def __validate!(tree)
        machines = [self, *tree.mounted_machines]
        machines.each do |machine|
          unplaced = machine.declared_checkpoints.values.reject { |handle| machine.nodes.key?(handle) }
          raise DefinitionError, "#{machine}: #{unplaced.join(', ')} declared but never placed" if unplaced.any?
        end
        if start_checkpoint && !root.nodes.key?(start_checkpoint)
          raise DefinitionError, "#{self}: start #{start_checkpoint} must be a checkpoint of the root region"
        end

        tree.all_nodes.each do |node|
          raise DefinitionError, "#{self}: final checkpoint #{node.label} can't accept commands" if node.final? && node.commands.any?
          raise DefinitionError, "#{self}: final checkpoint #{node.label} can't take events" if node.final? && node.ons.any?

          node.commands.each_value do |definition|
            __check_partition_keys!(definition.command_class, 'command')
            definition.checkpoint_targets.each { |target| __check_target!(tree, node, target) }
          end
          node.ons.each_value do |definition|
            __check_partition_keys!(definition.command_class, 'on event')
            definition.checkpoint_targets.each { |target| __check_target!(tree, node, target) }
          end
          __check_transient!(node) if node.transient?
          node.always.each { |eventless| __check_target!(tree, node, eventless.target) }
          __check_join!(tree, node) if node.dones.any? || node.regions.any?
          node.children_joins.each { |join| __check_children_join!(tree, node, join) }
          node.invokes.each { |spec| __check_invoke!(tree, node, spec) }
        end

        handled_messages_for_react.each { |klass| __check_partition_keys!(klass, 'reaction') }
        tree.all_regions.each { |region| __check_mount!(tree, region) if region.mount }
        tree.children_specs.each_value { |spec| __check_child!(tree, spec) }
      end

      # A mounted machine escalates through its root finals, mapped onto the
      # checkpoints reachable from the checkpoint that mounts it.
      # @param region [PlacedRegion]
      def __check_mount!(tree, region)
        region.on_done.each do |final, target|
          __check_root_final!(region.machine, final, "region #{region.path} on_done")
          tree.resolve(region.parent, target)
        end
      end

      def __check_invoke!(tree, node, spec)
        spec.on_done.each do |child_final, target|
          __check_root_final!(spec.machine, child_final, "invoke #{spec.machine}'s on_done")
          tree.resolve(node, target)
        end
      end

      def __check_root_final!(machine, handle, what)
        node = machine.root.nodes[handle]
        return if node&.final?

        raise DefinitionError, "#{self}: #{what} #{handle} isn't a final checkpoint of #{machine}'s root region"
      end

      def __check_child!(tree, spec)
        child = spec.machine
        missing = partition_keys - child.partition_keys
        unless missing.empty?
          raise DefinitionError, "#{self}: child #{child} must partition by our keys too, missing #{missing.join(', ')}"
        end

        spec.stop_on.each do |handle|
          raise DefinitionError, "#{self}: stop_on #{handle} isn't a checkpoint" unless tree.checkpoint?(handle)
        end
      end

      def __check_children_join!(tree, node, join)
        spec = tree.children_specs[join.as]
        raise DefinitionError, "#{self}: children_done #{join.as.inspect} doesn't name spawned children" unless spec && !spec.invoke?

        join.definition.checkpoint_targets.each { |target| __check_target!(tree, node, target) }
      end

      def __check_join!(tree, node)
        raise DefinitionError, "#{self}: #{node.label} declares done but has no regions" if node.regions.empty?

        node.dones.each do |eventless|
          target = tree.resolve(node, eventless.target)
          raise DefinitionError, "#{self}: #{node.label}'s done targets #{target.label}, inside its own regions" if target.descendant_of?(node)
          # Restoring itself (or an ancestor) would bring back the final regions that completed it.
          if History === eventless.target && (target.equal?(node) || node.descendant_of?(target))
            raise DefinitionError, "#{self}: #{node.label}'s done targets #{eventless.target}, which would restore its final regions and join again"
          end
        end
      end

      # A transient checkpoint is left as soon as it's entered.
      def __check_transient!(node)
        always = node.always.find { |eventless| !eventless.guarded? }.target
        prefix = "#{self}: #{node.label} is transient (always #{always}), so it can't"
        raise DefinitionError, "#{prefix} accept commands" if node.commands.any?
        raise DefinitionError, "#{prefix} take events" if node.ons.any?
        raise DefinitionError, "#{prefix} have timers" if node.afters.any?
        raise DefinitionError, "#{prefix} invoke children" if node.invokes.any?
        raise DefinitionError, "#{prefix} have regions" if node.regions.any?
        raise DefinitionError, "#{prefix} join with done or children_done" if node.dones.any? || node.children_joins.any?
      end

      def __check_partition_keys!(klass, role)
        names = __attribute_names(klass)
        missing = partition_keys - names
        return if missing.empty?

        raise DefinitionError, "#{self}: #{role} #{klass} must declare #{missing.join(', ')}"
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
          elsif handled_events.include?(msg.class) && instance.should_react?(instance.state, msg, replaying:)
            # Decided once, when claimed: replaying evolves what it decided.
            decided = instance.__decide_on(msg)
            actions = Actions.build_for(decided, guard:, source: msg)
            actions += Actions.build_for(instance.__react(msg), source: msg)
            actions += instance.collect_actions(state: instance.state, messages: [msg]) do |appended|
              { events: appended.first(decided.size) }
            end
            [actions, msg]
          elsif (__transition_classes.include?(msg.class) || child_event?(msg.class) || instance.reacts_to?(msg)) &&
                instance.should_react?(instance.state, msg, replaying:)
            # A child finishing can complete a join or map onto one of our
            # checkpoints, and being started can invoke a child of our own:
            # decided here, under the guard.
            joins = child_event?(msg.class) || __started?(msg) ? instance.__decide_eventless : NONE
            actions = Actions.build_for(joins, guard:, source: msg)
            actions += Actions.build_for(instance.__react(msg), source: msg)
            # The joins are the pair's first appends; what reactions dispatch isn't decided events.
            actions += instance.collect_actions(state: instance.state, messages: [msg]) do |appended|
              { events: appended.first(joins.size) }
            end
            [actions, msg]
          else
            [Actions::OK, msg]
          end
        end
      end

      def __started?(msg) = msg.class.equal?(@__started_class)
    end

    # The attributes of a transition: what moved the machine, and the paths it
    # left and entered.
    # @api private
    def self.__transition_attributes(message)
      message.attribute :trigger, String
      message.attribute :source, String
      message.attribute :target, String
      message.attribute :exited, Sourced::Types::Array[String]
      message.attribute :entered, Sourced::Types::Array[String]
    end

    attr_reader :partition_values

    # @param partition_values [Hash{Symbol => String}]
    def initialize(partition_values = {})
      self.class.validate!
      @partition_values = partition_values
      @runtime = Runtime.new(self.class.tree)
      @uncommitted_events = nil
      @handler = nil
    end

    # Read-only view of the runtime state: the current configuration, the
    # transitions that entered it, and whether it's final.
    # @return [View]
    def machine = View.new(@runtime)

    # The machine's definition as {.to_h} returns it, with this instance's
    # status filled in. See {Chart}.
    #
    # @return [Hash]
    def to_h = Chart.new(self.class, runtime: @runtime).to_h

    # Evolve messages in log order: transitions move the machine, then any
    # +evolve+ handler updates the domain state.
    def evolve(messages)
      Array(messages).each do |msg|
        @runtime.apply(msg) if self.class.__transition_classes.include?(msg.class)
        track_child(msg)
        super([msg])
        __region_projections.each { |region_projection| region_projection.evolve([msg]) }
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
      return stoppable? if stop_command?(command)
      return false if stale_timer?(command)

      handlers_for(command).any?
    end

    # Decide +command+ against the current configuration and state.
    #
    # The deepest active checkpoints that accept the command handle it: a
    # checkpoint's handler runs only when none of its active regions accepts the
    # command. Handlers in sibling regions run in definition order, each seeing
    # the events of the ones before it; one whose checkpoint an earlier handler
    # left doesn't run. Each handler moves the machine to one of its declared
    # checkpoints (or nowhere, when +nil+ is declared): the one it chooses with
    # {#to}, or its only one. Joins the decision completes follow it.
    #
    # @param command [Sourced::Message]
    # @return [Array<Sourced::Message>] the events produced
    # @raise [TransitionError] if a handler chose a checkpoint it didn't declare
    def decide(command)
      if stop_command?(command)
        return [] unless stoppable?

        return deciding do
          root = self.class.tree.root
          transition(@runtime.current_node(root), root.nodes.fetch(self.class.stop_checkpoint), trigger: 'stop')
          append_eventless
        end
      end

      handlers = handlers_for(command)
      return [] if handlers.empty?

      deciding do
        handlers.each do |node|
          run_handler(node, node.commands.fetch(command.class), command) if @runtime.active_node?(node)
        end
        append_eventless
      end
    end

    # What the current configuration decides without a command, until nothing
    # is left: +always+, mounted machines' +on_done+, +done+ joins,
    # +children_done+ joins, invoking children, and mapping invoked children's
    # results.
    # @api private
    # @return [Array<Sourced::Message>]
    def __decide_eventless = deciding { append_eventless }

    # Decide +event+, claimed, for the deepest active checkpoints that declare
    # it with +on+ (see {#decide}), then what that leaves to decide without one.
    # Nothing when no active checkpoint declares it.
    # @api private
    # @return [Array<Sourced::Message>]
    def __decide_on(event)
      handlers = handlers_for(event, :ons)
      return NONE if handlers.empty?

      deciding do
        handlers.each do |node|
          run_handler(node, node.ons.fetch(event.class), event) if @runtime.active_node?(node)
        end
        append_eventless
      end
    end

    # Produce a data event from within a command handler and apply it
    # immediately. Partition keys the event declares are filled in.
    #
    # @param event_class [Class, Symbol]
    # @param payload [Hash]
    # @return [Sourced::Message]
    # @raise [InvalidEventError] if the event isn't valid
    def event(event_class, payload = {})
      raise Error, "#{self.class}: events are produced by command handlers only" unless @uncommitted_events
      event_class = case event_class
                    in Class if event_class < Checkpoint
                      raise Error, "#{self.class}: #{event_class} is a checkpoint; " \
                                   "move to it with to(#{event_class.checkpoint_name.inspect})"
                    in Targets
                      raise Error, "#{self.class}: #{event_class.members.compact.join(' | ')} are checkpoints; move to one of them with to"
                    in Symbol then self.class[event_class]
                    else event_class
                    end

      __append(event_class.new(payload: with_partition_keys(event_class, payload)))
    end

    # Run the exit actions of the checkpoints +msg+ left (innermost first,
    # stopping the children they invoked), the entry actions and timers of the
    # ones it entered (stopping the children whose +stop_on+ they are), then
    # any +reaction+ to it.
    # @api private
    # @return [Array<Sourced::Message>] dispatched messages
    def __react(msg)
      __handling_reactions([msg]) do |message|
        if self.class.__transition_classes.include?(message.class)
          tree = self.class.tree
          message.payload.exited.each do |path|
            left = tree.node_at(path)
            left.exits.each { |blk| instance_exec(state, message, &blk) }
            left.invokes.each { |spec| stop_children(spec) }
          end
          entered = message.payload.entered.map { |path| tree.node_at(path) }
          entered.each do |node|
            node.entries.each { |blk| instance_exec(state, message, &blk) }
            start_timers(node, message)
          end
          tree.children_specs.each_value do |spec|
            stop_children(spec) if entered.any? { |node| spec.stop_on.include?(node.key) }
          end
        end

        method_name = Sourced.message_method_name(React::PREFIX, message.class.to_s)
        respond_to?(method_name) ? Array(send(method_name, state, message)).compact : EMPTY_ARRAY
      end
    end

    private

    # Choose where the running handler moves the machine: one of the
    # checkpoints its command declared.
    # @param target [Checkpoint, Symbol]
    def to(target)
      raise Error, "#{self.class}: to is for command, on and children_done handlers" unless @handler
      raise TransitionError, "#{self.class}: #{@handler[:what]} moved the machine twice" if @handler.key?(:target)

      @handler[:target] = @handler[:node].machine.send(:__target, target, declare: false)
    end

    def deciding
      @uncommitted_events = []
      yield
      @uncommitted_events
    ensure
      @uncommitted_events = nil
    end

    def __append(evt)
      raise InvalidEventError, "#{self.class}: invalid #{evt.class}: #{evt.errors.inspect}" unless evt.valid?

      @uncommitted_events << evt
      evolve([evt])
      evt
    end

    # Take a declared transition from +source+: to a checkpoint, or a {History}
    # of one, resolved from +from+ (the node that declares it).
    def move(source, target, trigger:, from: source)
      history = (target in History) ? target : nil
      transition(source, self.class.tree.resolve(from, target), trigger:, history:)
    end

    # Move from +source+ to +target+: append our class for +target+'s
    # checkpoint, and a +Finished+ when it reaches one of a child machine's
    # root finals.
    def transition(source, target, trigger:, history: nil)
      exited, entered = @runtime.transition_sets(source, target, history:)
      klass = self.class.checkpoint_class(target.key)
      __append(klass.new(payload: with_partition_keys(klass, {
        trigger:, source: source.path, target: target.path,
        exited: exited.map(&:path), entered: entered.map(&:path)
      })))
      final = entered.find { |node| node.final? && node.region.root? }
      return unless final && (finished = self.class.__finished_class)

      __append(finished.new(payload: with_partition_keys(finished, final: final.key.checkpoint_name.to_s)))
    end

    # The deepest active checkpoints accepting +message+, in definition order.
    # @param kind [Symbol] +:commands+ or +:ons+
    def handlers_for(message, kind = :commands)
      accepting = @runtime.active_nodes.select { |node| node.public_send(kind).key?(message.class) }
      accepting.reject { |node| accepting.any? { |other| other.descendant_of?(node) } }
    end

    # +always+ first: a transient checkpoint resolves before anything that
    # depends on where its region ends up (a mounted machine's escalation, a join).
    def append_eventless
      MAX_JOINS.times do
        return unless always_step || mount_done_step || join_step || children_join_step || invoke_step
      end
      raise Error, "#{self.class}: more than #{MAX_JOINS} joins in one decision"
    end

    # A mounted machine at one of its root finals that +on_done+ maps: move to
    # the mapped checkpoint. Before +done+ joins, so a region's escalation
    # (ex. a payment expiring) wins over completing its checkpoint.
    def mount_done_step
      @runtime.active_nodes.each do |node|
        node.regions.each do |region|
          current = @runtime.current_node(region)
          next unless current.final? && (target = region.on_done[current.key])

          move(current, target, trigger: 'on_done', from: node)
          return true
        end
      end
      false
    end

    # The projections of the machines mounted as regions, built from our
    # state: their +evolve+ blocks apply to the same object as ours.
    def __region_projections
      @__region_projections ||= self.class.tree.mounted_machines
                                    .map(&:projection_class)
                                    .reject { |klass| klass.handled_messages.empty? }
                                    .map { |klass| klass.new(projection.state) }
    end

    # The first active checkpoint with a passing +always+ moves on.
    def always_step
      @runtime.active_nodes.each do |node|
        eventless = node.always.find { |candidate| candidate.pass?(state) }
        next unless eventless

        move(node, eventless.target, trigger: 'always')
        return true
      end
      false
    end

    # An active checkpoint whose regions are all final takes its first
    # passing +done+, if any.
    def join_step
      @runtime.active_nodes.each do |node|
        next if node.dones.empty? || !node.regions.all? { |region| @runtime.current_node(region).final? }

        eventless = node.dones.find { |candidate| candidate.pass?(state) }
        next unless eventless

        move(node, eventless.target, trigger: 'done')
        return true
      end
      false
    end

    def children_join_step
      @runtime.active_nodes.each do |node|
        node.children_joins.each do |join|
          refs = @runtime.children(join.as)
          next if refs.empty? || !refs.all?(&:final?)

          run_handler(node, join.definition, nil)
          return true
        end
      end
      false
    end

    # An active checkpoint that invokes a child, with no child for its current
    # entry, starts one; one whose child finished at a mapped final checkpoint
    # moves to the mapped checkpoint.
    def invoke_step
      @runtime.active_nodes.each do |node|
        node.invokes.each do |spec|
          entry_id = @runtime.entered_by(node.region)&.id
          ref = @runtime.children(spec.as).find { |child| child.entry_id == entry_id }
          if ref.nil?
            start_child(spec, spec.keys.call(state).to_h)
            return true
          elsif ref.final? && (target = spec.on_done[ref.state])
            move(node, target, trigger: spec.machine::Finished.type)
            return true
          end
        end
      end
      false
    end

    # Run one handler: its block, then its transition, to the checkpoint the
    # block chose with {#to}, or else its only declared one.
    def run_handler(node, definition, message)
      what = message ? message.class.to_s : definition.command_class
      @handler = { node:, what: }
      instance_exec(state, message, &definition.block) if definition.block
      target = @handler.fetch(:target) { definition.auto? ? definition.targets.first : nil }
      @handler = nil

      if target.nil?
        return if definition.optional?

        raise TransitionError, "#{self.class}: #{what} must move to one of #{declared(definition)}"
      end
      unless definition.checkpoint_targets.include?(target)
        raise TransitionError, "#{self.class}: #{what} moved to #{target}, not one of #{declared(definition)}"
      end

      move(node, target, trigger: message ? message.type : definition.command_class)
    ensure
      @handler = nil
    end

    def declared(definition) = definition.targets.map { |t| t || 'nil' }.join(' | ')

    # Spawn a child declared with {.spawns}: append its +Started+, with our
    # partition keys filled in.
    #
    # @param as [Symbol]
    # @param payload [Hash] the child's own keys, and anything else, as its +Started+'s +input+
    # @return [Sourced::Message] the +Started+
    def spawn(as, payload = {})
      spec = self.class.tree.children_specs.fetch(as) { raise Error, "#{self.class} doesn't spawn #{as.inspect}" }
      raise Error, "#{self.class}: #{as.inspect} is invoked, not spawned" if spec.invoke?

      start_child(spec, payload)
    end

    def start_child(spec, payload)
      child = spec.machine
      klass = child::Started
      own = payload.to_h.slice(*child.partition_keys)
      paths = child.__start_paths
      __append(klass.new(payload: with_partition_keys(klass, {
        **own, input: payload.to_h.except(*child.partition_keys),
        trigger: 'start', source: INITIAL.to_s, target: paths.first, exited: [], entered: paths
      })))
    end

    def track_child(msg)
      self.class.tree.child_events.fetch(msg.class, NONE).each do |spec|
        child = spec.machine
        keys = msg.payload.to_h.slice(*(child.partition_keys - self.class.partition_keys))
        if msg.instance_of?(child::Started)
          entry_id = spec.invoke? ? invoking_entry(spec) : nil
          @runtime.track_child(spec, keys, state: child.start_checkpoint, final: false, entry_id:)
        else
          handle = child.declared_checkpoints.fetch(msg.payload.final.to_sym)
          @runtime.track_child(spec, keys, state: handle, final: true, entry_id: nil)
        end
      end
    end

    def invoking_entry(spec)
      node = self.class.tree.placed(spec.node).find { |placed| @runtime.active_node?(placed) }
      node && @runtime.entered_by(node.region)&.id
    end

    def stop_children(spec)
      @runtime.children(spec.as).reject(&:final?).each { |child| dispatch(spec.machine::Stop, child.keys) }
    end

    def stop_command?(command) = self.class.stop_checkpoint && command.instance_of?(self.class::Stop)

    # +Stop+ applies once the machine has started, until it's final.
    def stoppable?
      root = @runtime.current_node(self.class.tree.root)
      !root.initial? && !root.final?
    end

    # Timers for a checkpoint entry the machine has already left would only be
    # stale when delivered, so they aren't started.
    def start_timers(node, transition)
      return unless @runtime.active_entry_of?(node, transition.id)

      node.afters.each do |timer|
        started = @__reaction_dispatchers.size
        instance_exec(state, transition, &timer.block)
        @__reaction_dispatchers.drop(started).each do |dispatcher|
          dispatcher.at(timer.from(transition.created_at)).with_metadata(ENTERED_BY => transition.id, ENTERED_AT => node.path)
        end
      end
    end

    # A timer command was caused by the transition it names: dispatched in
    # reaction to it. Metadata is copied along correlation chains, so a command
    # that merely inherited the key names an event other than its cause, and
    # isn't a timer.
    def stale_timer?(command)
      started_by = command.metadata[ENTERED_BY] || command.metadata[ENTERED_BY.to_s]
      return false unless started_by && started_by == command.causation_id

      path = command.metadata[ENTERED_AT] || command.metadata[ENTERED_AT.to_s]
      return !@runtime.active_entry?(started_by) unless path

      node = self.class.tree.node_at(path)
      !@runtime.active_entry_of?(node, started_by)
    rescue Error
      true
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
  end
end

require 'sourced/machine/tree'
require 'sourced/machine/chart'
require 'sourced/machine/mermaid'
