# frozen_string_literal: true

require 'fugit'
require 'sourced/machine/definition'

module Sourced
  # Reactor base class for state machines: Sourced's take on statecharts.
  #
  # A machine's position is its *states*: handles declared with
  # {.State} and placed with {.state}, {.final} or {.started}. Each
  # state lists the commands it accepts, and the states each command
  # may move the machine to. A state is also an event class: every move
  # appends one event of the target state's class
  # (+<type_prefix>.runtime.<name>+), whose payload says which states it
  # left and entered; the machine's position is replayed from those. Domain
  # facts are data events, which only change the domain +context+:
  #
  #   class OrderMachine < Sourced::Machine
  #     partition_by :order_id
  #
  #     Placed    = State(:placed)
  #     Completed = State(:completed)
  #     Cancelled = State(:cancelled)
  #
  #     context { OrderState.new }
  #     evolve(ItemAdded) { |order, evt| order.add(evt.payload.sku) }
  #
  #     initial do
  #       command PlaceOrder => Placed
  #     end
  #
  #     state Placed do
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
  #         state :declined
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
  # A command moves the machine to one of its declared states, or nowhere
  # when the declaration includes +nil+ (or has no arrow). Commands no active
  # state accepts are refused: see {#handles_command?}.
  #
  # Like a {Decider}, a machine is one reactor and one consumer group: every
  # message for a partition is decided one at a time, under one consistency
  # guard, so regions never run concurrently and share the domain context. Entry
  # and exit actions are reactions: they run when the machine claims the
  # transition, after the deciding command has committed.
  class Machine
    include Sourced::Evolve
    include Sourced::React
    include Sourced::Sync
    extend Sourced::Consumer
    extend Definition

    # The machine's transitions, its children's starts and finishes, and the
    # evolve types of its parts are evolved too, alongside the
    # projection's evolve types. They're the machine's own, not its
    # projection's (which has no handlers for them), so adopting a projection
    # with +evolve SomeProjection+ keeps them. Prepended after Sourced::Evolve's
    # class methods, so +super+ is the projection's list.
    module EvolveTypes
      # Fixed by {Machine.validate!}; computed on each call before it.
      def handled_messages_for_evolve
        return @__evolve_types if @__evolve_types

        tree = self.tree
        parts = tree.mounted_machines.flat_map { |part| part.projection_class.handled_messages }
        (super + __transition_classes + tree.child_events + parts + handled_events).uniq
      end
    end
    singleton_class.prepend EvolveTypes

    # What the machine evolves and reacts to is fixed by {Machine.validate!}
    # (see {EvolveTypes}): declaring more afterwards would go unread.
    module DefinitionLock
      %i[context evolve before_evolve reaction].each do |dsl|
        define_method(dsl) do |*args, &block|
          raise DefinitionError, "#{self}: #{dsl} declared after the machine was validated" if @validated

          super(*args, &block)
        end
      end
    end
    singleton_class.prepend DefinitionLock

    Error = Class.new(Sourced::Error)

    # Raised when a machine's definition is invalid. Checked when the machine is
    # registered with a Router, and before its first instance is built.
    DefinitionError = Class.new(Error)

    # Raised when a handler moves the machine somewhere its command didn't declare.
    TransitionError = Class.new(Error)

    # Raised when a handler starts a child its +starts:+ doesn't declare, or
    # starts fewer children than it declares.
    StartError = Class.new(Error)

    # Raised when an +after+ block dispatches a command its timer doesn't declare.
    TimerError = Class.new(Error)

    # Raised when a machine builds an event that isn't valid.
    InvalidEventError = Class.new(Error)

    # Raised when a stored path names no state in the machine's definition,
    # ex. after a state or region was renamed or removed.
    UnresolvablePathError = Class.new(Error)

    INITIAL = :initial

    # Metadata keys on messages dispatched by an +after+ block: the id of the
    # transition that entered the state and started the timer, and the
    # state's path. One transition can enter several states, so a
    # timer belongs to the pair.
    ENTERED_BY = :entered_by
    ENTERED_AT = :entered_at

    # Joins chain (a join's state can complete an outer join), but a
    # definition can't make them chain forever. A safety net only.
    MAX_JOINS = 100

    # The base of the events {.stopped} generates (+Started+, +Finished+).
    ChildEvent = Class.new(Sourced::Event)

    # The constants {.stopped} generates.
    CHILD_MESSAGES = %i[Stop Started Finished].freeze

    NONE = [].freeze

    # A state: a position in a machine's chart, and the event recording
    # a transition to it. {Machine.State} generates one subclass per name,
    # the state's *handle*: the tree places it (once per placement of its
    # definition, so a part mounted twice has each of its states at two
    # paths), transitions target it, and the machine appends it. A part's
    # states are its owner's: typed by the owner's prefix, keyed by its
    # partition keys. Positions are the paths in the payload, never the class.
    class State < Sourced::Event
      # @return [Class<State>] itself: a handle, like a {History}'s +state+
      def self.state = self

      # @return [Class<State>] itself (see {Machine.state_handle})
      def self.to_state_handle = self

      # @return [Class<Machine>, Class<Part>] the machine or part that declares
      #   the state: for a part's own compound state (+Part::State+), the part
      def self.machine = @machine

      # @return [Class<Part>, nil] the part this is the compound state of ({Part.to_state_handle})
      def self.part = @part

      # @return [Symbol] unique within {.machine}
      def self.state_name = @state_name

      # @param other [Class<State>, History, Targets, nil]
      # @return [Targets]
      def self.|(other) = Targets.new(members: [self]) | other

      # A target re-entering this state where its regions were when it
      # was last left: each region's state (+deep+: the whole
      # configuration below it), or its default entry where it has none.
      # @return [History]
      def self.history(deep: false) = History.new(state:, deep:)

      def self.to_s = name || (state ? "#{machine}:#{state_name}" : super)
      def self.inspect = to_s

      # Generate a state class: +machine+'s partition keys and the transition
      # attributes. A type can be taken over only by a class for the same
      # machine (a reloaded or redefined one); any other clash raises, since
      # the registry would silently replace it.
      # @api private
      def self.__generate(type, machine:, state_name:, part: nil)
        existing = Sourced::Message.registry[type]
        unless existing.nil? || (Class === existing && existing < State && existing.machine.__type_owner_key == machine.__type_owner_key)
          raise DefinitionError, "#{machine}: message type #{type.inspect} is already #{existing}'s"
        end

        schema = machine.partition_schema
        klass = define(type) do
          schema.to_h.each { |key, key_type| attribute key.to_sym, key_type }
          Machine.__transition_attributes(self)
        end
        klass.instance_variable_set(:@machine, machine)
        klass.instance_variable_set(:@state_name, state_name)
        klass.instance_variable_set(:@part, part)
        klass
      end
    end

    # @return [Boolean] whether +value+ is a state handle
    def self.handle?(value) = Class === value && value < State

    # The state handle +value+ stands for, wherever the chart DSL takes one:
    # anything responding to +to_state_handle+ (a {State}, itself; a {Part},
    # its compound state). Symbols, Strings, +nil+, {History} and {Targets}
    # are returned as they are, for their own branches.
    # @param value [Object]
    # @return [Class<State>, Symbol, String, History, Targets, nil]
    def self.state_handle(value)
      case value
      in Symbol | String | nil | History | Targets then value
      else value.to_state_handle
      end
    end

    # The states a transition may go to; +nil+ means it may stay.
    Targets = Data.define(:members) do
      # @param other [State, History, Targets, nil]
      # @return [Targets]
      def |(other)
        if Class === other && other < Machine
          raise DefinitionError, "#{self} | #{other}: #{other} is a machine, and a union of states takes states"
        end

        case other
        in Targets then with(members: members | other.members)
        in nil | History then with(members: members | [other])
        else with(members: members | [other.to_state_handle])
        end
      end

      def to_s = members.map { |member| member.nil? ? 'nil' : member.to_s }.join(' | ')
      def inspect = to_s
    end

    # The children a handler may start ({Machine.|}): machine classes, and
    # +nil+ when starting any is optional.
    Starts = Data.define(:members) do
      # @param other [Class<Machine>, Starts, nil]
      # @return [Starts]
      def |(other)
        case other
        in Starts then with(members: members | other.members)
        in nil then with(members: members | [nil])
        in Class if other < Machine then with(members: members | [other])
        else
          raise DefinitionError, "#{self} | #{other.inspect}: a union of machines takes machines or nil"
        end
      end

      def to_s = members.map { |member| member.nil? ? 'nil' : member.to_s }.join(' | ')
      def inspect = to_s
    end

    # A history pseudo-target ({State.history}): +state+, entered
    # where its regions were when it was last left.
    History = Data.define(:state, :deep) do
      def |(other) = Targets.new(members: [self]) | other
      def to_s = deep ? "#{state}.history(deep: true)" : "#{state}.history"
      def inspect = to_s
    end

    # A command a state accepts, and the states it may move to.
    # +nil+ among the targets means "may stay". +starts+ are the children it
    # may start, +nil+ among them meaning starting any is optional.
    CommandDef = Data.define(:command_class, :targets, :block, :starts) do
      def initialize(starts: NONE, **) = super

      # Moves to its single target when the handler doesn't choose one.
      def auto? = targets.size == 1 && !targets.first.nil?
      def optional? = targets.include?(nil)
      def state_targets = targets.compact
      def started_machines = starts.compact
      # Without a block, starts its only child from the message's payload.
      def auto_start? = block.nil? && starts.size == 1 && !starts.first.nil?
      def must_start? = starts.any? && !starts.include?(nil)
    end

    # What a guard is called with: the domain context.
    GuardInterface = Types::Interface[:call]

    # The guard of an unguarded +always+ or +done+.
    UNGUARDED = ->(_context) { true }

    # A transition taken without a command (+always+, +done+), when its guard
    # passes. +guard+ is a +#call(context)+ (+UNGUARDED+ when declared without
    # one); +guard_label+ names it for rendering.
    Eventless = Data.define(:target, :guard, :guard_label) do
      # @param guard [Symbol, #call(context), nil] a Symbol is sent to the domain context
      # @return [Eventless]
      def self.build(target, guard)
        case guard
        in nil then new(target:, guard: UNGUARDED, guard_label: nil)
        in Symbol then new(target:, guard: ->(context) { context.public_send(guard) }, guard_label: guard.to_s)
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
      def pass?(context) = guard.call(context)
    end

    # A delayed action: runs when its state is entered, and future-dates
    # what it dispatches by +seconds+ from the entering transition. +delay+ is
    # as declared, seconds or a duration string, for rendering.
    # +commands+ are what it may dispatch; without a +block+, it dispatches
    # each of them.
    After = Data.define(:delay, :seconds, :commands, :block) do
      # @param time [Time] the entering transition's +created_at+
      # @return [Time] when what the block dispatches is delivered
      def from(time) = time + seconds
    end

    # A child machine the parent tracks: started by handlers that declare it in
    # +starts:+ (+kind+ +:spawn+, one spec per machine), or by a state
    # ({NodeBuilder#start}, +kind+ +:invoke+, one spec per placement).
    # +node+ (the state; a {PlacedNode} in the {Tree}'s), +payload+ and
    # +on_done+ are for a state's child; +stop_on+, the placed states whose
    # +stops+ name it, for handlers' (filled by the {Tree}).
    ChildSpec = Data.define(:machine, :kind, :stop_on, :node, :payload, :on_done) do
      def invoke? = kind == :invoke
    end

    # A child as its parent knows it: its own keys (beyond the parent's), the
    # state it started at, or the final one it finished at, and for a
    # child a state started, the path of that state (+at+, from its
    # +Started+) and the entry it was started for (nil +at+: a handler's).
    ChildRef = Data.define(:keys, :state, :final, :at, :entry_id) do
      def final? = final
    end

    # A join over children: once every child of +machine+ is final.
    ChildrenJoin = Data.define(:machine, :definition)

    # A region whose states are a part's ({Part}), declared with
    # +region :name, SomePart+ or, for the part's compound state,
    # +state SomePart+. Its definition isn't copied: the tree
    # places the part's root region under the state (see {Tree}).
    # +on_final+ maps the part's root final states onto the parent's
    # (filled while the part is defined). A compound region starts at the
    # part's +started+ state; any other at its initial position.
    RegionMount = Data.define(:name, :machine, :on_final, :compound) do
      def compound? = compound
    end

    # A set of states, one of them current while the region is active.
    # The root region is always active; any other belongs to a state, and
    # is active while that state is.
    class Region
      attr_reader :name, :parent, :nodes

      # @param name [Symbol]
      # @param parent [Node, nil] the state the region belongs to; nil for the root
      def initialize(name, parent)
        @name = name
        @parent = parent
        @nodes = {}
      end

      def root? = parent.nil?

      # The region's position before its first state, declared or not.
      def initial_node = nodes[INITIAL] || (@blank_initial ||= Node.new(INITIAL, final: false, region: self))

      def label = root? ? 'the root region' : "region #{name.inspect}"
    end

    # A state (or a region's initial position) and what it accepts.
    class Node
      attr_reader :key, :region, :commands, :ons, :entries, :exits, :afters, :regions, :invokes, :children_joins, :always, :dones,
                  :stops

      # @param key [State, Symbol] the state, or {INITIAL}
      # @param final [Boolean]
      # @param region [Region] the region the state belongs to
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
        @stops = []
      end

      def final? = @final
      def initial? = key == INITIAL
      # Left as soon as it's entered: it has an unguarded +always+.
      def transient? = always.any? { |eventless| !eventless.guarded? }
      def label = initial? ? "#{region.label}'s initial position" : key.to_s
    end

    # Evaluates a region's block: +initial+, +state+, +final+.
    class RegionBuilder
      def initialize(machine, region)
        @machine = machine
        @region = region
      end

      def initial(&block) = @machine.send(:__define_node, @region, INITIAL, final: false, &block)

      def state(handle, *rest, &block)
        @machine.send(:__check_single_state!, handle, rest)
        @machine.send(:__define_node, @region, handle, final: false, &block)
      end

      def final(handle, &block) = @machine.send(:__define_node, @region, handle, final: true, &block)

      # @see Machine.State
      def State(name) = @machine.State(name)
    end

    # Evaluates a state's block: +command+, +on+, +entry+, +exit+, +after+,
    # +region+, +done+, +always+, +start+, +children_done+, +stops+.
    class NodeBuilder
      def initialize(machine, node)
        @machine = machine
        @node = node
      end

      # @see Machine.State
      def State(name) = @machine.State(name)

      # @overload command(command_class => target, &block)
      #   @param target [State, Symbol, Targets, Array, nil] a state, a union of them, or +nil+
      # @overload command(command_class, &block)
      #   Data only: same as +command_class => nil+.
      #
      # +starts:+ declares the children the handler may start with
      # {Machine#start}: a machine, or a union of them (+A | B+, +A | nil+).
      # Without a block, +starts: A+ starts one +A+ from the command's payload.
      def command(declaration = nil, starts: nil, **arrow, &block)
        definition = @machine.send(:__command_definition, __declaration(declaration, arrow), block, starts:)
        if @node.commands.key?(definition.command_class)
          raise DefinitionError, "#{@machine}: #{@node.label} accepts #{definition.command_class} twice"
        end

        @node.commands[definition.command_class] = definition
      end

      # Move when the machine claims +event_class+, appended by another reactor,
      # while this state is active. Decided like a command: the same
      # targets, +to+ and +nil+; the block gets +(context, event)+. Skipped when
      # replaying.
      #
      # @overload on(event_class => target, &block)
      # @overload on(event_class, &block) data only
      def on(declaration = nil, starts: nil, **arrow, &block)
        definition = @machine.send(:__command_definition, __declaration(declaration, arrow), block, what: 'on', starts:)
        event_class = definition.command_class
        case event_class
        in Class if event_class < State
          raise DefinitionError, "#{@machine}: on #{event_class}: a state class; on takes another reactor's event"
        in Class if event_class < ChildEvent
          raise DefinitionError, "#{@machine}: on #{event_class}: a child machine's event; use start … on_done or children_done"
        in Class if event_class < Sourced::Event
          nil
        else
          raise DefinitionError, "#{@machine}: on takes an event, got #{event_class}"
        end
        raise DefinitionError, "#{@machine}: #{@node.label} takes #{event_class} twice" if @node.ons.key?(event_class)

        @node.ons[event_class] = definition
      end

      # Runs when the machine enters this state, as a reaction.
      # @yield [context, transition]
      def entry(&block)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so no entry actions" if @node.initial?

        @node.entries << block
      end

      # Runs when the machine leaves this state, as a reaction to the
      # transition that left it.
      # @yield [context, transition]
      def exit(&block) = @node.exits << block

      # Dispatches +commands+ when the machine enters this state, as a
      # reaction. They're delivered +delay+ after the entering transition, and
      # carry its id: a command delivered after the machine has left that
      # state entry is stale, and refused (see {Machine#handles_command?}).
      #
      #   state AwaitingPayment do
      #     after '1h' => ExpirePayment
      #     command ExpirePayment => Expired
      #   end
      #
      # Without a block, each declared command is dispatched with the
      # partition keys as its payload. A block dispatches them itself, and only
      # the declared ones ({TimerError}):
      #
      #   after('1h' => Remind | ExpirePayment) { |order, _transition| dispatch Remind, email: order[:email] }
      #
      # The delay is seconds, or a duration string as {Sourced::Message#at}
      # takes it (+'30m'+, +'1h30m'+, +'P1D'+), up to weeks: months and years
      # aren't a fixed number of seconds.
      #
      # @param declaration [Hash{Numeric, String => Class, Plumb::Or}] delay => command(s)
      # @yield [context, transition]
      def after(declaration, &block)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so no timers" if @node.initial?

        delay, declared =
          case declaration
          in Hash if declaration.size == 1 then declaration.first
          else raise DefinitionError, "#{@machine}: after declares what it dispatches, ex. after('1h' => Command), got #{declaration.inspect}"
          end
        seconds = __timer_seconds(delay)
        @node.afters << After.new(delay:, seconds:, commands: __timer_commands(delay, declared).uniq, block:)
      end

      # Declare a region, active while this state is. A state with
      # regions is parallel: all of its regions are active at once.
      #
      # With a block, the region's states are declared inline. With a
      # part ({Part}), they're that part's: its states are ours, its context
      # ours (its +evolve+ blocks apply to it too). It escalates through its
      # root final states, which +on_final+ maps onto ours.
      #
      #   region :payment, PaymentPart, on_final: { PaymentPart::Expired => Cancelled }
      #
      # @param name [Symbol] unique within the machine
      # @param machine [Class<Part>, nil]
      # @param on_final [Hash{State => State}] the machine's root final => our state
      def region(name, machine = nil, on_final: {}, &block)
        raise DefinitionError, "#{@machine}: the initial position can't have regions" if @node.initial?
        raise DefinitionError, "#{@machine}: final state #{@node.key} can't have regions" if @node.final?
        if @node.regions.any? { |r| RegionMount === r && r.compound? }
          raise DefinitionError, "#{@machine}: #{@node.key} is a part's compound state, so it can't have regions"
        end

        if machine
          raise DefinitionError, "#{@machine}: region #{name.inspect} takes a part or a block, not both" if block

          @machine.send(:__mount_region, @node, name.to_sym, machine, on_final)
        else
          raise DefinitionError, "#{@machine}: region #{name.inspect}: on_final is for a mounted part" if on_final.any?

          @machine.send(:__define_region, @node, name.to_sym, &block)
        end
      end

      # Join: once every region of this state is at a final state,
      # the machine moves to +target+, in the same decision. Several guarded
      # joins are tried in order, the first passing one taken; with none
      # passing (and no unguarded one), the machine stays until a later
      # decision passes one.
      #
      #   done Delivered, if: :all_confirmed?
      #   done Partial
      #
      # @param target [State, Symbol] a state of this region or an ancestor's
      # @param guard [Symbol, #call, nil] +if:+, of the domain context
      def done(target, if: nil)
        @node.dones << @machine.send(:__eventless, @node, 'done', target, binding.local_variable_get(:if), @node.dones)
      end

      # An eventless transition: as soon as this state is entered, and
      # after every decision while it's active, the first +always+ whose guard
      # passes moves the machine, in the same decision. A state with an
      # unguarded +always+ is transient: it can't accept commands or have
      # timers.
      #
      #   state Reviewing do
      #     always Cancelled, if: ->(trip) { trip.any?('cancelled') }
      #     always Confirmed, if: :all_confirmed?
      #     always AwaitingSuppliers
      #   end
      #
      # @param target [State, Symbol]
      # @param guard [Symbol, #call, nil] +if:+, of the domain context
      def always(target, if: nil)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so no always" if @node.initial?
        raise DefinitionError, "#{@machine}: final state #{@node.label} can't have always" if @node.final?

        @node.always << @machine.send(:__eventless, @node, 'always', target, binding.local_variable_get(:if), @node.always)
      end

      # For a part's compound state (+state SomePart+): when the part
      # reaches one of its root finals, move to the target mapped to it, before
      # any +done+ join. Unmapped finals leave the machine there.
      #
      #   state Verification do
      #     on_final Verification::Verified => Active
      #   end
      #
      # @param mapping [Hash{State => State}] the machine's root final => our state
      def on_final(mapping)
        mount = @node.regions.find { |region| RegionMount === region && region.compound? }
        raise DefinitionError, "#{@machine}: #{@node.label} mounts no part, so no on_final; use done" unless mount

        @machine.send(:__map_finals, mount, mapping)
      end

      # Run a child machine while this state is active: started in the
      # decision that enters the state, sent its +Stop+ when the state
      # is left, and its final states mapped onto ours. Each placement of
      # the state (a part mounted twice) starts, maps and stops its own.
      #
      #   state AwaitingPayment do
      #     start PaymentMachine do
      #       payload { |order, state| { payment_id: order.payment_id, amount: order.total } }
      #       on_done PaymentMachine::Captured => Paid
      #       on_done PaymentMachine::Declined => PaymentFailed
      #     end
      #   end
      #
      # @param child [Class<Machine>] a machine with +started+ and +stopped+
      # @yield evaluated in a {StartBuilder}: +payload+ (required) and +on_done+
      def start(child, &block)
        raise DefinitionError, "#{@machine}: #{@node.label} can't start children" if @node.initial? || @node.final?
        raise DefinitionError, "#{@machine}: start #{child} needs a block with payload { |context, state| … }" unless block

        builder = StartBuilder.new(@machine, child).tap { |b| b.instance_eval(&block) }
        raise DefinitionError, "#{@machine}: start #{child} needs payload { |context, state| … }" unless builder.payload_block

        @machine.send(:__define_invoke, child, node: @node, payload: builder.payload_block, on_done: builder.on_done_pairs)
      end

      # Join over started children: once every child of +Child+ its handlers
      # started (+starts:+; not a state's) is at a final state, and at least
      # one was started, the machine moves to the declared state, as a command
      # handler would.
      #
      #   children_done LineMachine => LinesDone | LinesPartiallyFailed do |order|
      #     to(machine.children(LineMachine).all? { |line| line.state == LineMachine::Picked } ? LinesDone : LinesPartiallyFailed)
      #   end
      def children_done(declaration = nil, starts: nil, **arrow, &block)
        declaration = __declaration(declaration, arrow)
        unless declaration in Hash and declaration.size == 1
          raise DefinitionError, "#{@machine}: children_done takes { Child => state }, got #{declaration.inspect}"
        end

        child, target = declaration.first
        unless Class === child && child < Machine
          raise DefinitionError, "#{@machine}: children_done takes a child machine, got #{child.inspect}"
        end

        what = "children_done #{child.group_label}"
        targets = @machine.send(:__flatten_targets, target)
        raise DefinitionError, "#{@machine}: #{what} must move to a state, not nil" if targets.include?(nil)
        raise DefinitionError, "#{@machine}: #{what} with several states needs a block" if block.nil? && targets.size > 1

        starts = @machine.send(:__starts, starts, block, what)
        definition = CommandDef.new(command_class: what, targets: targets.uniq, block:, starts:)
        @node.children_joins << ChildrenJoin.new(machine: child, definition:)
      end

      # Stop every running child of +child+ handlers started when this state
      # is entered, sending each its +Stop+. Some handler must declare
      # +starts: child+. A state's children stop when it's left.
      # @param child [Class<Machine>]
      def stops(child)
        raise DefinitionError, "#{@machine}: the initial position is never entered, so it stops no children" if @node.initial?
        raise DefinitionError, "#{@machine}: stops takes a child machine, got #{child.inspect}" unless Class === child && child < Machine
        raise DefinitionError, "#{@machine}: #{@node.label} stops #{child} twice" if @node.stops.include?(child)

        @node.stops << child
      end

      # +command Cmd => A, starts: X+ passes the arrow as keywords; +command Cmd, starts: X+ positionally.
      private def __declaration(declaration, arrow)
        return arrow if declaration.nil?
        raise DefinitionError, "#{@machine}: invalid declaration #{declaration.inspect}, #{arrow.inspect}" if arrow.any?

        declaration
      end

      # +after+'s delay in seconds: as given, or a duration string's.
      private def __timer_seconds(delay)
        return delay if Numeric === delay

        duration = Fugit.parse_duration(delay) if String === delay
        unless duration
          raise DefinitionError, "#{@machine}: after needs seconds or a duration string (ex. '30m', 'PT1H'), got #{delay.inspect}"
        end
        if duration.h.key?(:yea) || duration.h.key?(:mon)
          raise DefinitionError, "#{@machine}: after(#{delay.inspect}) can't wait months or years, which vary in length; give days (ex. '30d')"
        end

        duration.to_sec
      end

      # +after('1h' => A | B)+: a command, or a union of them.
      private def __timer_commands(delay, declared)
        case declared
        in Plumb::Or then declared.children.flat_map { __timer_commands(delay, it) }
        in Class if declared < Sourced::Command then [declared]
        else raise DefinitionError, "#{@machine}: after(#{delay.inspect}) dispatches commands, got #{declared.inspect}"
        end
      end
    end

    # Evaluates a state-level +start+'s block: +payload+ and +on_done+.
    class StartBuilder
      attr_reader :payload_block, :on_done_pairs

      def initialize(machine, child)
        @machine = machine
        @child = child
        @payload_block = nil
        @on_done_pairs = []
      end

      # The child's +Started+ payload: its own keys (ours are filled in), and
      # anything else as its +input+. Called once, when the child is started.
      # @yieldparam context the domain context
      # @yieldparam state [State, ChildEvent] the event that entered
      #   this state: a state event, or this machine's +Started+ for
      #   the state a parent starts it at
      def payload(&block)
        raise DefinitionError, "#{@machine}: start #{@child}'s payload needs a block" unless block
        raise DefinitionError, "#{@machine}: start #{@child} declares payload twice" if @payload_block

        @payload_block = block
      end

      # Map the child's root finals onto targets of this state.
      # @param mapping [Hash{State => State}] child final => our target
      def on_done(mapping)
        raise DefinitionError, "#{@machine}: start #{@child}'s on_done takes { Final => target }, got #{mapping.inspect}" unless Hash === mapping && mapping.any?

        @on_done_pairs.concat(mapping.to_a)
      end
    end

    # A machine's configuration: where its regions are, read against its
    # definition. {.configuration} builds one from the paths of the active
    # states (+machine.paths+ of a loaded instance, or the +exited+ / +entered+
    # paths of its state events, kept by a projection), to inspect a machine
    # without loading its history. It has no domain context, history,
    # children or entry events, so it tells what the active states accept,
    # not whether a command would succeed.
    class Configuration
      # @param machine [Class<Machine>]
      # @param runtime [Runtime]
      def initialize(machine, runtime)
        @machine = machine
        @runtime = runtime
      end

      # Duck-typed: anything standing for a configuration answers it (a machine, its {#machine}).
      # @return [Configuration] itself
      def to_machine_config = self

      # The paths of the active states, outermost first: what {.configuration} takes.
      # @return [Array<String>]
      def paths = @runtime.active_nodes.reject(&:initial?).map(&:path)

      # The command classes the active states accept, and +Stop+ while it
      # applies (the machine has started and isn't final).
      #
      # @return [Set<Class>]
      def available_commands
        commands = @runtime.active_nodes.flat_map { |node| node.commands.keys }
        commands << @machine::Stop if @machine.stop_state && !@runtime.current_node(@runtime.root).initial? && !final?
        commands.to_set.freeze
      end

      # Every command class the machine accepts, in any state, and +Stop+ for
      # a child: {Machine.handled_commands}, as a Set.
      #
      # @return [Set<Class>]
      def handled_commands = @machine.__command_set

      # The current configuration: a state (or +:initial+), or for a
      # state with regions, +{ state => { region_name => value, … } }+,
      # and for a compound one, +{ state => value }+.
      def value = @runtime.value

      # @overload in?(state)
      #   Whether +state+ is active, in any region.
      # @overload in?(path)
      #   Whether the state at +path+ (+'reserving.suppliers.hotel.holding'+) is active.
      # @overload in?(region_name, state)
      #   Whether a region named +region_name+ is active and at +state+ (or +:initial+).
      def in?(*args)
        case args
        in [Class => state]
          state = Machine.state_handle(state)
          @runtime.active_nodes.any? { |node| node.key == state }
        in [String => path]
          @runtime.active_nodes.any? { |node| node.path == path }
        in [Symbol => region_name, state]
          state = Machine.state_handle(state)
          @runtime.active_regions.any? { |r| r.name == region_name && @runtime.current_node(r).key == state }
        end
      end

      # Whether the root region is at a final state.
      def final? = @runtime.current_node(@runtime.root).final?

      # The state the root region, or the active region named
      # +region_name+, is at: a handle, or +:initial+; nil when no region of
      # that name is active.
      def current(region_name = nil)
        region = region_name ? @runtime.active_regions.find { |r| r.name == region_name } : @runtime.root
        region && @runtime.current_node(region).key
      end
    end

    # Read-only view of a machine's runtime state, available as {#machine}:
    # its {Configuration}, and the transitions and children behind it.
    class View < Configuration
      # @overload children(child)
      #   @return [Array<ChildRef>] the children of machine +child+, started by handlers or by any state (see {ChildRef#at})
      # @overload children
      #   @return [Hash{Class<Machine> => Array<ChildRef>}] every child, by machine
      def children(child = nil) = child ? @runtime.children(child) : @runtime.all_children

      # The transition that entered the current state of the root region,
      # or of the active region named +region_name+ (nil while it's at its
      # initial position or inactive).
      def entered_by(region_name = nil)
        region = region_name ? @runtime.active_regions.find { |r| r.name == region_name } : @runtime.root
        region && @runtime.entered_by(region)
      end
    end

    # A loaded machine, as returned by {.load}. +value+ is the current
    # configuration (see {View#value}), +children+ its children by machine, and
    # +context+ the domain context.
    Snapshot = Data.define(:value, :context, :children)

    # The machine's own bookkeeping, separate from the domain context: per
    # region, the current state, the transition that entered it and the
    # states entered since the region was; and the children.
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

      # Record a child's start or finish. A state's child keeps the
      # placement and entry it was first started for.
      def track_child(child, keys, state:, final:, at:, entry_id:)
        refs = (@children[child] ||= {})
        known = refs[keys]
        refs[keys] = known ? known.with(state:, final:) : ChildRef.new(keys:, state:, final:, at:, entry_id:)
      end

      def children(child) = (@children[child] || {}).values
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

      # @return [Array<PlacedNode>] the active states, outermost first, regions in definition order
      def active_nodes = active_nodes_in(root)

      # @return [Array<PlacedRegion>] the active regions, outermost first, in definition order
      def active_regions = active_nodes.flat_map(&:regions).unshift(root)

      # Whether +event_id+ entered a state that's still active.
      def active_entry?(event_id)
        active_regions.any? { |region| position(region).entered_by&.id == event_id }
      end

      # Whether +node+ is active, entered by +event_id+.
      def active_entry_of?(node, event_id) = active_node?(node) && entered_by(node.region)&.id == event_id

      def value = value_of(root)

      # Position the regions at the states at +paths+, outermost first, as
      # {Configuration} takes them. For a fresh runtime: nothing is recorded
      # as entering them.
      #
      # @param paths [Array<String>] the paths of the active states
      # @raise [UnresolvablePathError] if a path doesn't resolve
      # @raise [ArgumentError] for an initial position, a state in an inactive region, or two states in one region
      def place(paths)
        nodes = paths.map { |path| @tree.node_at(path) }
        nodes.sort_by { |node| node.region.depth }.each do |node|
          raise ArgumentError, "#{@root.machine}: #{node.path} is an initial position, not a state" if node.initial?
          raise ArgumentError, "#{@root.machine}: #{node.path} is in an inactive region" unless active?(node.region)

          pos = position(node.region)
          unless pos.current == INITIAL
            raise ArgumentError, "#{@root.machine}: #{node.path} and #{current_node(node.region).path} are in one region"
          end

          pos.current = node.key
          pos.visited << node.key
        end
        self
      end

      # What moving from +source+ to +target+ leaves and enters, given the
      # current configuration. The transition's domain is the innermost region
      # that holds both: +source+'s region or an ancestor's, or for a target
      # below +source+, the one of +source+'s regions it's in. Everything
      # active in the domain is left, innermost first; the path from the domain
      # down to +target+ is entered, outermost first, then +target+'s default
      # configuration: each compound state's start, all the way down.
      #
      # With +history+, +target+'s regions are entered where they were when
      # it was last left (counting what this transition leaves), instead.
      #
      # @param source [PlacedNode] the active state (or initial position) taking the transition
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

      # Apply a transition, decided now or replayed: each entered state
      # becomes its region's current one, entered by +event+, and its regions
      # start afresh.
      #
      # @param event [State] a state event (or a child's +Started+)
      # @raise [UnresolvablePathError] if a path doesn't resolve
      # @raise [Error] if a path enters an inactive region
      def apply(event)
        # A region's history is where it was when its state was last left.
        event.payload.exited.each { |path| @tree.node_at(path, event).regions.each { |region| remember(region) } }
        event.payload.entered.each do |path|
          node = @tree.node_at(path, event)
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

      # +node+, then each region at its last state (deep: restored all the
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
      # This machine and the children it starts, including its parts',
      # so {Sourced.register} registers them with it (see {Config.register}).
      # @return [Array<Class>]
      def to_sourced_reactors = [self, *tree.children]

      # @param klass [Class]
      def child_event?(klass) = tree.child_event?(klass)

      # The final state this machine's +Stop+ moves it to.
      # @return [State, nil]
      attr_reader :stop_state

      # Every state class this machine appends (its own and its parts'), and
      # its +Started+ for a child: what a projection subscribes to for every move.
      # @return [Array<Class>]
      def transition_classes = __transition_classes

      # Declare what this machine becomes when its parent stops it. Generates a
      # +Stop+ command (+self::Stop+, keyed by this machine's partition keys),
      # accepted from any started, non-final position, that moves to +handle+,
      # a final state of the root region. With a block, places that final
      # with it; without one, marks an already-placed final, or places a bare
      # one. Also generates +Started+ and +Finished+, the events a parent
      # tracks its children by. Leaving that way runs exit actions as usual.
      # @param handle [State, Symbol]
      def stopped(handle, &block)
        raise DefinitionError, "#{self}: stopped is declared twice" if @stop_state
        raise DefinitionError, 'a machine with stopped needs a name' unless name
        raise DefinitionError, "#{self}: declare partition_by before stopped" if partition_keys.empty?
        if (taken = CHILD_MESSAGES.find { |constant| const_defined?(constant, false) })
          raise DefinitionError, "#{self}: stopped generates #{taken}, which is already defined; name that constant differently"
        end

        @stop_state = __handle(handle)
        node = block ? final(@stop_state, &block) : (nodes[@stop_state] || final(@stop_state))
        unless node.final? && node.region.root?
          raise DefinitionError, "#{self}: stopped #{@stop_state} must be a final state of the root region"
        end

        schema = partition_schema
        const_set(:Stop, Sourced::Command.define("#{type_prefix}.runtime.child.stop") { schema.to_h.each { |key, key_type| attribute key.to_sym, key_type } })
        __define_child_messages
      end

      # A union of machines, for a handler's +starts:+: +starts: A | B+,
      # +starts: A | nil+. +nil+ goes last (+nil | A+ is +NilClass#|+).
      # @param other [Class<Machine>, Starts, nil]
      # @return [Starts]
      def |(other) = Starts.new(members: [self]) | other

      # Before any state or mount: their classes take the keys.
      def partition_by(*keys, **typed)
        __check_declared_first!(:partition_by)
        super
      end

      # Before any state or mount: their types take the prefix.
      def message_prefix(prefix = nil)
        __check_declared_first!(:message_prefix) if prefix
        super
      end

      # Before any state or mount: their types take the prefix.
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

      # Prefix of the generated message types: {.message_prefix}, by default
      # derived from +group_id+: +Shop::OrderMachine+ => +shop.order_machine+.
      def type_prefix = message_prefix

      # The machine's definition placed into one tree (see {Tree}), which the
      # runtime, routing and rendering work on. Built once the machine is
      # validated; before that (while the class body runs), built on each call.
      # @return [Tree]
      def tree = @tree || Tree.new(self)

      # @return [Array<PlacedNode>] every node in the machine's tree, initial positions included
      def all_nodes = tree.all_nodes

      # Fixed by {.validate!}; computed on each call before it.
      # @return [Array<Class>] every command any state accepts, and +Stop+
      def handled_commands
        return @__handled_commands if @__handled_commands

        commands = all_nodes.flat_map { |node| node.commands.keys }
        commands << self::Stop if stop_state
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
        (handled_commands + handled_events + __transition_classes + tree.child_events + handled_messages_for_react).uniq
      end

      # Check the definition. Memoised; runs before the first instance is built,
      # and when the machine is registered with a Router.
      #
      # @raise [DefinitionError]
      # @return [true]
      def validate!
        # +@validating+: a child that starts its parent back.
        return true if @validated || @validating

        @validating = true
        raise DefinitionError, "#{self} needs partition_by" if partition_keys.empty?
        raise DefinitionError, 'a machine needs a name or a consumer_group' unless group_id

        tree = Tree.new(self)
        __validate!(tree)
        # Children are started from here, so they must be valid too.
        tree.children.each(&:validate!)
        @tree = tree
        tree.mounted_machines.each(&:__lock!)
        # What the runtime reads per message, fixed now that the definition is.
        @__handled_commands = handled_commands.freeze
        @__handled_events = handled_events.freeze
        @__evolve_types = handled_messages_for_evolve.freeze
        @__start_paths = __start_paths.freeze if start_state
        @__transition_classes = __transition_classes.freeze
        @__transition_set = @__transition_classes.to_set.freeze
        @__command_set = @__handled_commands.to_set.freeze
        @__event_set = @__handled_events.to_set.freeze
        @__region_projection_classes = __region_projection_classes.freeze
        @__eventless_steps = __eventless_steps.freeze
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
      # states they left and entered.
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
        evolvable = new_messages.select { |msg| types.include?(msg.to_message.class) }
        __process(partition_values, new_messages, history.messages + evolvable, guard: history.guard, replaying:)
      end

      # The configuration at the states at +paths+, read-only: ex. what the
      # active states accept, for a record a projection keeps the paths of.
      #
      #   OrderMachine.configuration(['placed', 'placed.payment.paying']).available_commands
      #
      # @param paths [Array<String>] the paths of the active states, as {Configuration#paths} returns them
      # @return [Configuration]
      # @raise [UnresolvablePathError] if a path doesn't resolve
      # @raise [ArgumentError] for an initial position, a state in an inactive region, or two states in one region
      def configuration(paths)
        validate!
        Configuration.new(self, Runtime.new(tree).place(paths))
      end

      # An instance at the states at +paths+, without loading its history: ex.
      # to decide commands against a status stored as paths, and store the
      # paths it moves to. Its context is +context+, or else the +context+
      # block's, and it has no history, children or events that entered its
      # states, so a placed state's +start+ counts its child as not started yet.
      #
      #   group = ConsumerGroupMachine.at(['active'], { retry_count: 0 }, group_id: 'g1')
      #   group.decide(stop) if group.handles_command?(stop)
      #   group.machine.paths # => ['stopped']
      #   group.context       # => { retry_count: 0 }
      #
      # @param paths [Array<String>] the paths of the active states, as {Configuration#paths} returns them
      # @param context [Object] the starting context, as given: deciding evolves this object
      # @param partition_values [Hash{Symbol => String}]
      # @return [Machine]
      # @raise [Sourced::MissingPartitionKeyError] if a partition key is missing or nil
      # @raise [UnresolvablePathError] if a path doesn't resolve
      # @raise [ArgumentError] for an initial position, a state in an inactive region, or two states in one region
      def at(paths, context = Projection::UNSET, **partition_values)
        missing = partition_keys.select { |key| partition_values[key].nil? }
        if missing.any?
          raise Sourced::MissingPartitionKeyError, "#{self} can't be placed without values for partition keys #{missing.inspect}"
        end

        new(partition_values).__place(paths, context)
      end

      # Load a machine instance and return its configuration and domain context.
      #
      # @param store [Sourced::Store, nil] defaults to +Sourced.store+
      # @return [Snapshot]
      def load(store: nil, **values)
        instance, = Sourced.load(self, store:, **values)
        Snapshot.new(value: instance.machine.value, context: instance.context, children: instance.machine.children)
      end

      # The events that move this machine: its states, its parts' (and the
      # compound states of those it mounts as one), and for a child,
      # +Started+. Fixed by {.validate!}; computed on each call before it.
      # @api private
      def __transition_classes
        return @__transition_classes if @__transition_classes

        tree = self.tree
        compound = tree.all_regions.select(&:compound?).map { |region| region.machine.to_state_handle }
        [*declared_states.values, *tree.mounted_machines.flat_map { |part| part.declared_states.values }, *compound.uniq, *@__started_class]
      end

      # Membership for the runtime, by message class: Sets fixed by {.validate!}.
      # @api private
      def __transition_class?(klass) = (@__transition_set || __transition_classes).include?(klass)
      # @api private
      def __handled_command?(klass) = (@__command_set || handled_commands).include?(klass)
      # @api private
      # @return [Set<Class>] {.handled_commands}, as a Set
      def __command_set = @__command_set || handled_commands.to_set.freeze
      # @api private
      def __handled_event?(klass) = (@__event_set || handled_events).include?(klass)

      # The eventless steps the definition can take, in the order they're tried:
      # see {Machine#append_eventless}.
      # @api private
      # @return [Array<Symbol>]
      def __eventless_steps
        return @__eventless_steps if @__eventless_steps

        nodes = tree.all_nodes
        {
          always_step: nodes.any? { |node| node.always.any? },
          mount_final_step: tree.all_regions.any? { |region| region.on_final.any? },
          join_step: nodes.any? { |node| node.dones.any? },
          children_join_step: nodes.any? { |node| node.children_joins.any? },
          invoke_step: nodes.any? { |node| node.invokes.any? }
        }.select { |_, present| present }.keys
      end

      # The projections of the parts that evolve anything.
      # @api private
      def __region_projection_classes
        @__region_projection_classes ||
          tree.mounted_machines.map(&:projection_class).reject { |klass| klass.handled_messages.empty? }
      end

      # What a parent's +Started+ enters: this machine's start, and its default configuration.
      # @api private
      # @return [Array<String>]
      def __start_paths = @__start_paths || tree.default_entry_paths(start_state)

      # @api private
      # @return [Class, nil] +Finished+, for a machine with +stopped+
      def __finished_class = @__finished_class

      # What tells a state class's owner apart from a redefinition of it (see {State.__generate}).
      # @api private
      def __type_owner_key = group_id

      private

      def __state_type(name)
        raise DefinitionError, 'a machine needs a name or a consumer_group before its states' unless group_id

        "#{type_prefix}.runtime.#{name}"
      end

      def __check_declared_first!(what)
        return if declared_states.empty? && mounts.empty?

        raise DefinitionError, "#{self}: declare #{what} before states and mounts"
      end

      def __define_child_messages
        schema = partition_schema
        const_set(:Started, ChildEvent.define("#{type_prefix}.runtime.child.started") do
          schema.to_h.each { |key, key_type| attribute key.to_sym, key_type }
          Machine.__transition_attributes(self)
          attribute :input, Sourced::Types::Hash.default { {} }
          # The parent's state that started it, for the parent to tell placements apart.
          attribute :parent_state, (Sourced::Types::String | Sourced::Types::Nil).default(nil)
        end)
        @__started_class = self::Started
        @__finished_class = const_set(:Finished, ChildEvent.define("#{type_prefix}.runtime.child.finished") do
          schema.to_h.each { |key, key_type| attribute key.to_sym, key_type }
          attribute :final, String
        end)
      end

      # @param node [PlacedNode]
      def __check_target!(tree, node, target)
        handle = target.state
        placed = tree.resolve(node, target)
        return if placed.machine == node.machine || placed.descendant_of?(node)

        raise DefinitionError,
              "#{self}: #{node.label} targets #{handle}, another machine's state that isn't below it; " \
              'a part reaches ours through on_final'
      end

      def __validate!(tree)
        [self, *tree.mounted_machines].each do |machine|
          unplaced = machine.declared_states.values.reject { |handle| machine.nodes.key?(handle) }
          raise DefinitionError, "#{machine}: #{unplaced.join(', ')} declared but never placed" if unplaced.any?
        end
        if start_state && !root.nodes.key?(start_state)
          raise DefinitionError, "#{self}: start #{start_state} must be a state of the root region"
        end

        accepted = handled_commands.to_set
        tree.all_nodes.each do |node|
          raise DefinitionError, "#{self}: final state #{node.label} can't accept commands" if node.final? && node.commands.any?
          raise DefinitionError, "#{self}: final state #{node.label} can't take events" if node.final? && node.ons.any?

          node.commands.each_value do |definition|
            __check_auto_start!(definition, __attribute_names(definition.command_class), "command #{definition.command_class}")
            definition.state_targets.each { |target| __check_target!(tree, node, target) }
          end
          node.ons.each_value do |definition|
            __check_auto_start!(definition, __attribute_names(definition.command_class), "on #{definition.command_class}")
            definition.state_targets.each { |target| __check_target!(tree, node, target) }
          end
          __check_transient!(node) if node.transient?
          node.afters.each { |timer| __check_timer!(node, timer, accepted) }
          node.always.each { |eventless| __check_target!(tree, node, eventless.target) }
          __check_join!(tree, node) if node.dones.any? || node.regions.any?
          node.children_joins.each { |join| __check_children_join!(tree, node, join) }
          node.invokes.each { |spec| __check_invoke!(tree, node, spec) }
          node.stops.each do |child|
            next if tree.spawned(child)

            raise DefinitionError, "#{self}: #{node.label} stops #{child}, but no handler declares starts: #{child}"
          end
        end

        tree.all_regions.each { |region| __check_mount!(tree, region) if region.mount }
        tree.children.each { |child| __check_child!(child) }
        __check_part_prefixes!(tree)
        __check_part_evolves!(tree)
        PartitionSchema.check!(self, error: DefinitionError)
      end

      # A part's prefix makes its message and state types: two parts sharing
      # one would claim the same types (anonymous ones without the registry
      # noticing).
      def __check_part_prefixes!(tree)
        tree.mounted_machines.group_by(&:part_prefix).each do |prefix, parts|
          next if parts.size < 2

          raise DefinitionError,
                "#{self}: #{parts.join(' and ')} share the part prefix #{prefix.inspect}, so their messages and states would have the same types"
        end
      end

      # A part's evolve blocks are ours: one per message class, across us and our parts.
      def __check_part_evolves!(tree)
        evolved = projection_class.handled_messages.to_h { |klass| [klass, self] }
        tree.mounted_machines.each do |part|
          part.projection_class.handled_messages.each do |klass|
            raise DefinitionError, "#{self}: #{klass} is evolved by both #{evolved[klass]} and #{part}" if evolved.key?(klass)

            evolved[klass] = part
          end
        end
      end

      # A part escalates through its root finals, mapped onto the
      # states reachable from the state that mounts it.
      # @param region [PlacedRegion]
      def __check_mount!(tree, region)
        region.on_final.each do |final, target|
          __check_root_final!(region.machine, final, "region #{region.path} on_final")
          tree.resolve(region.parent, target)
        end
      end

      def __check_invoke!(tree, node, spec)
        spec.on_done.each do |child_final, target|
          __check_root_final!(spec.machine, child_final, "start #{spec.machine}'s on_done")
          tree.resolve(node, target)
        end
      end

      def __check_root_final!(machine, handle, what)
        node = machine.root.nodes[handle]
        return if node&.final?

        raise DefinitionError, "#{self}: #{what} #{handle} isn't a final state of #{machine}'s root region"
      end

      def __check_child!(child)
        missing = partition_keys - child.partition_keys
        unless missing.empty?
          raise DefinitionError, "#{self}: child #{child} must partition by our keys too, missing #{missing.join(', ')}"
        end
      end

      def __check_children_join!(tree, node, join)
        unless tree.spawned(join.machine)
          raise DefinitionError, "#{self}: #{join.definition.command_class}: no handler declares starts: #{join.machine}"
        end

        # No message to take a started child's keys from: only ours.
        __check_auto_start!(join.definition, partition_keys, join.definition.command_class)
        join.definition.state_targets.each { |target| __check_target!(tree, node, target) }
      end

      # A blockless handler starting a child takes the child's keys from its
      # message (+names+), beyond ours.
      def __check_auto_start!(definition, names, label)
        return unless definition.auto_start?

        child = definition.starts.first
        missing = child.partition_keys - partition_keys - names
        return if missing.empty?

        raise DefinitionError,
              "#{self}: #{label} starts #{child} without a block, so it must declare #{child}'s keys #{missing.join(', ')}"
      end

      # A timer's commands are accepted somewhere, or they'd never be handled.
      # Without a block, they're built from the partition keys alone.
      def __check_timer!(node, timer, accepted)
        label = "#{node.label}'s after(#{timer.delay.inspect})"
        timer.commands.each do |command|
          raise DefinitionError, "#{self}: #{label} dispatches #{command}, which no state accepts" unless accepted.include?(command)
          next if timer.block

          missing = (command::Payload._schema.resolve({}).errors || {}).keys - partition_keys
          next if missing.empty?

          raise DefinitionError,
                "#{self}: #{label} dispatches #{command} without a block, so it can't fill #{command}'s #{missing.join(', ')}"
        end
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

      # A transient state is left as soon as it's entered.
      def __check_transient!(node)
        always = node.always.find { |eventless| !eventless.guarded? }.target
        prefix = "#{self}: #{node.label} is transient (always #{always}), so it can't"
        raise DefinitionError, "#{prefix} accept commands" if node.commands.any?
        raise DefinitionError, "#{prefix} take events" if node.ons.any?
        raise DefinitionError, "#{prefix} have timers" if node.afters.any?
        raise DefinitionError, "#{prefix} start children" if node.invokes.any?
        raise DefinitionError, "#{prefix} have regions" if node.regions.any?
        raise DefinitionError, "#{prefix} join with done or children_done" if node.dones.any? || node.children_joins.any?
      end

      def __attribute_names(klass)
        klass.respond_to?(:payload_attribute_names) ? klass.payload_attribute_names : []
      end

      def __process(values, messages, full_history, guard:, replaying:)
        instance = new(values)
        instance.evolve(full_history)

        each_with_partial_ack(messages) do |msg|
          klass = msg.to_message.class
          if __handled_command?(klass)
            next [Actions::OK, msg] unless instance.handles_command?(msg)

            events = instance.decide(msg)
            actions = Actions.build_for(events, guard:, source: msg)
            # The runner correlates the events as it appends them; +events:+ is
            # bound then, so the hooks see the events as stored.
            actions += instance.collect_actions(context: instance.context, messages: [msg]) do |appended|
              { events: appended }
            end
            [actions, msg]
          elsif __handled_event?(klass) && instance.should_react?(instance.context, msg, replaying:)
            # Decided once, when claimed: replaying evolves what it decided.
            decided = instance.__decide_on(msg)
            actions = Actions.build_for(decided, guard:, source: msg)
            actions += Actions.build_for(instance.__react(msg), source: msg)
            actions += instance.collect_actions(context: instance.context, messages: [msg]) do |appended|
              { events: appended.first(decided.size) }
            end
            [actions, msg]
          elsif (__transition_class?(klass) || child_event?(klass) || instance.reacts_to?(msg)) &&
                instance.should_react?(instance.context, msg, replaying:)
            # A child finishing can complete a join or map onto one of our
            # states, and being started can enter a state that starts
            # a child of our own: decided here, under the guard.
            joins = child_event?(klass) || klass.equal?(@__started_class) ? instance.__decide_eventless : NONE
            actions = Actions.build_for(joins, guard:, source: msg)
            actions += Actions.build_for(instance.__react(msg), source: msg)
            # The joins are the pair's first appends; what reactions dispatch isn't decided events.
            actions += instance.collect_actions(context: instance.context, messages: [msg]) do |appended|
              { events: appended.first(joins.size) }
            end
            [actions, msg]
          else
            [Actions::OK, msg]
          end
        end
      end
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
      @available_commands = nil
    end

    # Read-only view of the runtime state: the current configuration, the
    # transitions that entered it, and whether it's final.
    # @return [View]
    def machine = View.new(self.class, @runtime)

    # Place a fresh instance at +paths+, on +context+: see {.at}.
    # @api private
    # @return [self]
    def __place(paths, context = Projection::UNSET)
      @projection = self.class.projection_class.new(context) unless Projection::UNSET.equal?(context)
      @runtime.place(paths)
      moved!
      self
    end

    # The machine's definition as {.to_h} returns it, with this instance's
    # status filled in. See {Chart}.
    #
    # @return [Hash]
    def to_h = Chart.new(self.class, runtime: @runtime).to_h

    # Evolve messages in log order: transitions move the machine, then any
    # +evolve+ handler updates the domain context.
    def evolve(messages)
      Array(messages).each do |msg|
        if self.class.__transition_class?(msg.to_message.class)
          @runtime.apply(msg)
          moved!
        end
        track_child(msg)
        super([msg])
        __region_projections.each { |region_projection| region_projection.evolve([msg]) }
      end
      context
    end

    # The command classes the active states accept, and +Stop+ while it
    # applies (see {Configuration#available_commands}). Memoized until the
    # machine next transitions.
    #
    # @return [Set<Class>]
    def available_commands = @available_commands ||= machine.available_commands

    # Its configuration, read live (see {Configuration#to_machine_config}).
    # @return [View]
    def to_machine_config = machine

    # Every command class the machine accepts, in any state (see {Configuration#handled_commands}).
    #
    # @return [Set<Class>]
    def handled_commands = self.class.__command_set

    # Whether an active state accepts +command+. A command dispatched by an
    # +after+ block is refused once the machine has left the state entry
    # that started it, even if an active state accepts it.
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

    # Decide +command+ against the current configuration and context.
    #
    # The deepest active states that accept the command handle it: a
    # state's handler runs only when none of its active regions accepts the
    # command. Handlers in sibling regions run in definition order, each seeing
    # the events of the ones before it; one whose state an earlier handler
    # left doesn't run. Each handler moves the machine to one of its declared
    # states (or nowhere, when +nil+ is declared): the one it chooses with
    # {#to}, or its only one. Joins the decision completes follow it.
    #
    # @param command [Sourced::Message]
    # @return [Array<Sourced::Message>] the events produced
    # @raise [TransitionError] if a handler chose a state it didn't declare
    def decide(command)
      if stop_command?(command)
        return [] unless stoppable?

        return deciding do
          root = self.class.tree.root
          transition(@runtime.current_node(root), root.nodes.fetch(self.class.stop_state), trigger: 'stop')
          append_eventless
        end
      end

      handlers = handlers_for(command)
      return [] if handlers.empty?

      deciding do
        handlers.each do |node|
          run_handler(node, node.commands.fetch(command.to_message.class), command) if @runtime.active_node?(node)
        end
        append_eventless
      end
    end

    # What the current configuration decides without a command, until nothing
    # is left: +always+, mounted parts' +on_final+, +done+ joins,
    # +children_done+ joins, starting states' children, and mapping their
    # results.
    # @api private
    # @return [Array<Sourced::Message>]
    def __decide_eventless = deciding { append_eventless }

    # Move to +target+ from the current configuration as a transition would,
    # with no handler and nothing decided after it: the innermost active
    # region on the way to +target+ leaves its current state. The GWT
    # helpers' +at+.
    # @api private
    # @param target [PlacedNode]
    # @param trigger [String]
    # @return [Array<Sourced::Message>] +target+'s state, and a +Finished+ at a child machine's root final
    def __move_to(target, trigger:)
      region = target.region.lineage.find { |candidate| @runtime.active?(candidate) }
      deciding { transition(@runtime.current_node(region), target, trigger:) }
    end

    # Decide +event+, claimed, for the deepest active states that declare
    # it with +on+ (see {#decide}), then what that leaves to decide without one.
    # Nothing when no active state declares it.
    # @api private
    # @return [Array<Sourced::Message>]
    def __decide_on(event)
      handlers = handlers_for(event, :ons)
      return NONE if handlers.empty?

      deciding do
        handlers.each do |node|
          run_handler(node, node.ons.fetch(event.to_message.class), event) if @runtime.active_node?(node)
        end
        append_eventless
      end
    end

    # Produce a data event from within a command handler and apply it
    # immediately. Its partition keys are filled in: it must declare all of
    # them (see #__append).
    #
    # @param event_class [Class, Symbol]
    # @param payload [Hash]
    # @return [Sourced::Message]
    # @raise [InvalidEventError] if the event isn't valid
    def event(event_class, payload = {})
      raise Error, "#{self.class}: events are produced by command handlers only" unless @uncommitted_events
      event_class = case event_class
                    in Class if event_class < State
                      raise Error, "#{self.class}: #{event_class} is a state; " \
                                   "move to it with to(#{event_class.state_name.inspect})"
                    in Class if event_class < Part
                      raise Error, "#{self.class}: #{event_class} is a compound state; move to it with to(#{event_class})"
                    in Targets
                      raise Error, "#{self.class}: #{event_class.members.compact.join(' | ')} are states; move to one of them with to"
                    in Symbol then self.class[event_class]
                    else event_class
                    end

      __append(event_class.new(payload: with_partition_keys(event_class, payload)))
    end

    # Run the exit actions of the states +msg+ left (innermost first,
    # stopping the children they started), the entry actions and timers of the
    # ones it entered (stopping the children their +stops+ name), then
    # any +reaction+ to it.
    # @api private
    # @return [Array<Sourced::Message>] dispatched messages
    def __react(msg)
      __handling_reactions([msg]) do |message|
        if self.class.__transition_class?(message.to_message.class)
          tree = self.class.tree
          message.payload.exited.each do |path|
            left = tree.node_at(path, message)
            left.exits.each { |blk| instance_exec(context, message, &blk) }
            left.invokes.each { |spec| stop_children(spec.machine, at: left.path) }
          end
          entered = message.payload.entered.map { |path| tree.node_at(path, message) }
          entered.each do |node|
            node.entries.each { |blk| instance_exec(context, message, &blk) }
            start_timers(node, message)
          end
          entered.flat_map(&:stops).uniq.each { |child| stop_children(child, at: nil) }
        end

        method_name = self.class.reaction_handlers[message.to_message.class]
        method_name ? Array(send(method_name, context, message)).compact : EMPTY_ARRAY
      end
    end

    private

    # The configuration changed: what it accepts is read again.
    def moved! = @available_commands = nil

    # Choose where the running handler moves the machine: one of the
    # states its command declared.
    # @param target [State, Symbol]
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

      check_partition!(evt)
      @uncommitted_events << evt
      evolve([evt])
      evt
    end

    # Take a declared transition from +source+: to a state, or a {History}
    # of one, resolved from +from+ (the node that declares it).
    def move(source, target, trigger:, from: source)
      history = (target in History) ? target : nil
      transition(source, self.class.tree.resolve(from, target), trigger:, history:)
    end

    # Move from +source+ to +target+: append +target+'s state, and a
    # +Finished+ when it reaches one of a child machine's root finals.
    def transition(source, target, trigger:, history: nil)
      exited, entered = @runtime.transition_sets(source, target, history:)
      klass = target.key
      __append(klass.new(payload: with_partition_keys(klass, {
        trigger:, source: source.path, target: target.path,
        exited: exited.map(&:path), entered: entered.map(&:path)
      })))
      final = entered.find { |node| node.final? && node.region.root? }
      return unless final && (finished = self.class.__finished_class)

      __append(finished.new(payload: with_partition_keys(finished, final: final.key.state_name.to_s)))
    end

    # The deepest active states accepting +message+, in definition order.
    # @param kind [Symbol] +:commands+ or +:ons+
    def handlers_for(message, kind = :commands)
      accepting = @runtime.active_nodes.select { |node| node.public_send(kind).key?(message.to_message.class) }
      accepting.reject { |node| accepting.any? { |other| other.descendant_of?(node) } }
    end

    # +always+ first: a transient state resolves before anything that
    # depends on where its region ends up (a mounted part's escalation, a join).
    # Only the steps the definition can take ({.__eventless_steps}), over the
    # active states of each pass.
    def append_eventless
      steps = self.class.__eventless_steps
      return if steps.empty?

      MAX_JOINS.times do
        active = @runtime.active_nodes
        return unless steps.any? { |step| send(step, active) }
      end
      raise Error, "#{self.class}: more than #{MAX_JOINS} joins in one decision"
    end

    # A mounted part at one of its root finals that +on_final+ maps: move to
    # the mapped state. Before +done+ joins, so a region's escalation
    # (ex. a payment expiring) wins over completing its state.
    def mount_final_step(active)
      active.each do |node|
        node.regions.each do |region|
          next if region.on_final.empty?

          current = @runtime.current_node(region)
          next unless current.final? && (target = region.on_final[current.key])

          move(current, target, trigger: 'on_final', from: node)
          return true
        end
      end
      false
    end

    # The projections of our parts, built from our context: their +evolve+
    # blocks apply to the same object as ours.
    def __region_projections
      @__region_projections ||= self.class.__region_projection_classes.map { |klass| klass.new(projection.context) }
    end

    # The first active state with a passing +always+ moves on.
    def always_step(active)
      active.each do |node|
        eventless = node.always.find { |candidate| candidate.pass?(context) }
        next unless eventless

        move(node, eventless.target, trigger: 'always')
        return true
      end
      false
    end

    # An active state whose regions are all final takes its first
    # passing +done+, if any.
    def join_step(active)
      active.each do |node|
        next if node.dones.empty? || !node.regions.all? { |region| @runtime.current_node(region).final? }

        eventless = node.dones.find { |candidate| candidate.pass?(context) }
        next unless eventless

        move(node, eventless.target, trigger: 'done')
        return true
      end
      false
    end

    def children_join_step(active)
      active.each do |node|
        node.children_joins.each do |join|
          refs = @runtime.children(join.machine).select { |ref| ref.at.nil? }
          next if refs.empty? || !refs.all?(&:final?)

          run_handler(node, join.definition, nil)
          return true
        end
      end
      false
    end

    # An active state that starts a child, with no child for its current
    # entry, starts one; one whose child finished at a mapped final state
    # moves to the mapped state.
    def invoke_step(active)
      active.each do |node|
        node.invokes.each do |spec|
          entered_by = @runtime.entered_by(node.region)
          ref = @runtime.children(spec.machine).find { |child| child.at == node.path && child.entry_id == entered_by.id }
          if ref.nil?
            start_child(spec, spec.payload.call(context, entered_by).to_h)
            return true
          elsif ref.final? && (target = spec.on_done[ref.state])
            move(node, target, trigger: spec.machine::Finished.type)
            return true
          end
        end
      end
      false
    end

    # Run one handler: its block (or, without one, the start of its only
    # child), then its transition, to the state the block chose with
    # {#to}, or else its only declared one.
    def run_handler(node, definition, message)
      what = message ? message.to_message.class.to_s : definition.command_class
      @handler = { node:, what:, starts: definition.starts, started: 0 }
      if definition.block
        instance_exec(context, message, &definition.block)
      elsif definition.auto_start?
        start(definition.starts.first, message ? message.payload.to_h.except(*self.class.partition_keys) : {})
      end
      if definition.must_start? && @handler[:started].zero?
        raise StartError, "#{self.class}: #{what} must start #{definition.starts.size > 1 ? 'one of ' : ''}#{Starts.new(members: definition.starts)}"
      end

      target = @handler.fetch(:target) { definition.auto? ? definition.targets.first : nil }
      @handler = nil

      if target.nil?
        return if definition.optional?

        raise TransitionError, "#{self.class}: #{what} must move to one of #{declared(definition)}"
      end
      unless definition.state_targets.include?(target)
        raise TransitionError, "#{self.class}: #{what} moved to #{target}, not one of #{declared(definition)}"
      end

      move(node, target, trigger: message ? message.type : definition.command_class)
    ensure
      @handler = nil
    end

    def declared(definition) = definition.targets.map { |t| t || 'nil' }.join(' | ')

    # Start a child the running handler declares in its +starts:+: append
    # its +Started+, with our partition keys filled in.
    #
    #   command RequestVisas, starts: VisaMachine | nil do |tour, _cmd|
    #     tour.travellers.each { |id| start VisaMachine, traveller_id: id }
    #   end
    #
    # @param child [Class<Machine>]
    # @param payload [Hash] the child's own keys, and anything else, as its +Started+'s +input+
    # @return [Sourced::Message] the +Started+
    # @raise [StartError] if the handler's +starts:+ doesn't declare +child+
    def start(child, payload = {})
      raise Error, "#{self.class}: start is for command, on and children_done handlers" unless @handler
      unless Class === child && @handler[:starts].include?(child)
        raise StartError, "#{self.class}: #{@handler[:what]} starts #{child.inspect}, which its starts: doesn't declare"
      end

      @handler[:started] += 1
      start_child(self.class.tree.spawned(child), payload)
    end

    # A state's child's +Started+ names the placement that started it
    # (+parent_state+), which its +Finished+ maps back to through its keys.
    def start_child(spec, payload)
      child = spec.machine
      klass = child::Started
      own = payload.to_h.slice(*child.partition_keys)
      paths = child.__start_paths
      started = klass.new(payload: with_partition_keys(klass, {
        **own, input: payload.to_h.except(*child.partition_keys), parent_state: spec.invoke? ? spec.node.path : nil,
        trigger: 'start', source: INITIAL.to_s, target: paths.first, exited: [], entered: paths
      }))
      if spec.invoke? && (known = @runtime.children(child).find { |ref| ref.keys == child_keys(child, started) })
        raise StartError,
              "#{self.class}: #{spec.node.path} starts #{child} #{known.keys}, already started by #{known.at || 'a handler'}: " \
              "each start needs a child of its own"
      end

      __append(started)
    end

    def child_keys(child, msg) = msg.payload.to_h.slice(*(child.partition_keys - self.class.partition_keys))

    def track_child(msg)
      child = self.class.tree.child_for(msg.to_message.class)
      return unless child

      keys = child_keys(child, msg)
      if msg.to_message.instance_of?(child::Started)
        at = msg.payload.parent_state
        @runtime.track_child(child, keys, state: child.start_state, final: false, at:, entry_id: at && invoking_entry(at, msg))
      else
        handle = child.declared_states.fetch(msg.payload.final.to_sym)
        @runtime.track_child(child, keys, state: handle, final: true, at: nil, entry_id: nil)
      end
    end

    def invoking_entry(path, msg)
      node = self.class.tree.node_at(path, msg)
      @runtime.active_node?(node) ? @runtime.entered_by(node.region)&.id : nil
    end

    # Stop the running children of +child+ the placed state at +at+ started, or handlers' (+at+ nil).
    def stop_children(child, at:)
      @runtime.children(child).each { |ref| dispatch(child::Stop, ref.keys) if ref.at == at && !ref.final? }
    end

    def stop_command?(command) = self.class.stop_state && command.to_message.instance_of?(self.class::Stop)

    # +Stop+ applies once the machine has started, until it's final.
    def stoppable?
      root = @runtime.current_node(self.class.tree.root)
      !root.initial? && !root.final?
    end

    # Timers for a state entry the machine has already left would only be
    # stale when delivered, so they aren't started.
    def start_timers(node, transition)
      return unless @runtime.active_entry_of?(node, transition.id)

      node.afters.each do |timer|
        started = @__reaction_dispatchers.size
        if timer.block
          instance_exec(context, transition, &timer.block)
        else
          timer.commands.each { |command| dispatch command }
        end
        @__reaction_dispatchers.drop(started).each do |dispatcher|
          dispatched = dispatcher.message.class
          unless timer.commands.include?(dispatched)
            raise TimerError, "#{self.class}: #{node.path}'s after(#{timer.delay.inspect}) dispatched #{dispatched}, " \
                              "but declares #{timer.commands.join(' | ')}"
          end

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

      node = self.class.tree.node_at(path, command)
      !@runtime.active_entry_of?(node, started_by)
    end

    # Partition keys are filled in on dispatched messages too.
    def dispatch(message_class, payload = {})
      message_class = self.class[message_class] if message_class.is_a?(Symbol)
      super(message_class, with_partition_keys(message_class, payload))
    end

    # What the machine appends belongs to its partition: it declares every
    # partition key, with this partition's value. An event declaring only some
    # keys would be read into every partition sharing them.
    def check_partition!(evt)
      names = evt.class.payload_attribute_names
      self.class.partition_keys.each do |key|
        unless names.include?(key)
          raise InvalidEventError, "#{self.class}: #{evt.class} doesn't declare partition key #{key}, so it can't be appended to a #{self.class} partition"
        end
        next if evt.payload[key].to_s == partition_values[key].to_s

        raise InvalidEventError,
              "#{self.class}: #{evt.class} has #{key} #{evt.payload[key].inspect}, but is appended to the partition where #{key} is #{partition_values[key].inspect}"
      end
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
require 'sourced/machine/part'
