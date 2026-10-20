# frozen_string_literal: true

module Sourced
  class Machine
    # The chart definition DSL, and what the {Tree}, the runtime and the
    # renderers read from a definition: a machine's, or a part's. Both
    # {Machine} and {Part} extend it. Each supplies the hooks it calls:
    # +__state_type(name)+, +__type_owner_key+ and +__chart_owner+.
    module Definition
      # The root region: the machine's top-level states.
      # @return [Region]
      def root = @root ||= Region.new(:root, nil)

      # @return [Hash{Symbol => State}] the states this machine declares, by name
      def declared_states = @declared_states ||= {}

      # @return [Hash{State => Node}] the states this machine places, in any of its regions
      def nodes = @nodes ||= {}

      # @return [Hash{Symbol => Region}] every region but the root, declared inline
      def regions = @regions ||= {}

      # @return [Hash{Symbol => RegionMount}] regions that are parts
      def mounts = @mounts ||= {}

      # The children this machine's own handlers start (+starts:+), as
      # declared (the {Tree} merges its parts', and fills +stop_on+). A
      # state's +start+ is on its node ({Node#invokes}).
      # @return [Hash{Class<Machine> => ChildSpec}]
      def children_specs = @children_specs ||= {}

      # The state a parent starts this machine at, with +Started+.
      # @return [State, nil]
      attr_reader :start_state

      # Declare a state: a handle naming a position in this machine's
      # chart, and the event recording a transition to it, of type
      # +<type_prefix>.runtime.<name>+. Place it with {.state}, {.final}
      # or {.started}.
      #
      #   Placed = State(:placed)
      #
      # Declare +partition_by+ and +consumer_group+ first: the class takes the
      # partition keys, and its type the prefix.
      #
      # @param name [Symbol]
      # @return [Class<State>] the same handle for the same name
      def State(name)
        name = name.to_sym
        raise DefinitionError, "#{self}: a state can't be named #{INITIAL.inspect}" if name == INITIAL
        if name.to_s.include?('.')
          raise DefinitionError, "#{self}: state names can't contain dots, got #{name.inspect}"
        end

        declared_states[name] ||= begin
          if @validated
            raise DefinitionError, "#{self}: state #{name.inspect} declared after the machine was validated"
          end

          State.__generate(__state_type(name), machine: self, state_name: name)
        end
      end

      # Declare where a parent starts this machine (with +Started+), without a
      # command, or where a compound state mounting this part starts. With a
      # part, the start is a compound state, entered at that part's own start.
      # @param handle [State, Symbol]
      def started(handle, machine = nil, &block)
        raise DefinitionError, "#{self}: started is declared twice" if @start_state

        @start_state = __handle(handle)
        state(@start_state, machine, &block)
      end

      # The machine's position before its first state.
      def initial(&block) = __define_node(root, INITIAL, final: false, &block)

      # Place a state, and declare the commands it accepts.
      #
      # With a part ({Part}), the state is compound: its sub-states are that
      # part's states, entered at its +started+ state:
      #
      #   state Planning, PlanningPart do
      #     on_final PlanningPart::Finalised => Reserving
      #   end
      #
      # The block declares the compound state's own commands and actions.
      # The part's states are this machine's, its context this machine's; it
      # escalates through its root finals, which +on_final+ maps onto our states.
      #
      # @param handle [State, Symbol] one of this machine's states; a Symbol declares it
      # @param machine [Class<Part>, nil] a part with +started+, for a compound state
      def state(handle, machine = nil, &block)
        __define_state(root, handle, machine, &block)
      end

      # Place a final state. It accepts no commands.
      # @param handle [State, Symbol]
      def final(handle, &block) = __define_node(root, __handle(handle), final: true, &block)

      private

      def __define_state(region, handle, machine, &block)
        handle = __handle(handle)
        return __define_node(region, handle, final: false, &block) if machine.nil?

        __check_mountable!("state #{handle}", machine)
        raise DefinitionError, "#{self}: state #{handle}: #{machine} has no start state" unless machine.start_state

        # Named after its state, so one part can be several compound states.
        mount = __mount(handle.state_name, machine, compound: true)
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

      def __mount_region(node, name, machine, on_final)
        __check_mountable!("region #{name.inspect}", machine)
        mount = __mount(name, machine, compound: false)
        __map_finals(mount, on_final) if on_final.any?
        node.regions << mount
        mount
      end

      # Record a mount of +machine+ under +name+, unique among this machine's regions.
      def __mount(name, machine, compound:)
        raise DefinitionError, "#{self}: region #{name.inspect} is declared twice" if regions.key?(name) || mounts.key?(name)
        raise DefinitionError, "#{self}: region #{name.inspect}: a part can't be its own region" if machine <= self

        mounts[name] = RegionMount.new(name:, machine:, on_final: {}, compound:)
      end

      # Only parts mount, and only in their owner's chart.
      # @param at [String] where it's mounted, for errors
      def __check_mountable!(at, machine)
        case machine
        in Class if machine < Part
          return if machine.owner.equal?(__chart_owner)

          raise DefinitionError,
                "#{self}: #{at}: can't mount #{machine}: it's a part of #{machine.owner}, " \
                "so it mounts only in #{machine.owner} or its parts"
        in Class if machine < Machine
          raise DefinitionError,
                "#{self}: #{at}: can't mount #{machine}, a Sourced::Machine: to split #{__chart_owner}'s chart, " \
                "make it a Sourced::Machine::Part (part_of #{__chart_owner}); to reuse it with its own partition, " \
                'start it as a child (start, starts:)'
        else
          raise DefinitionError, "#{self}: #{at}: #{machine.inspect} isn't a Sourced::Machine::Part"
        end
      end

      # The machine whose chart this is: this machine, or a part's owner.
      def __chart_owner = self

      # Add +mapping+ (the part's root final => our target) to +mount+.
      def __map_finals(mount, mapping)
        unless Hash === mapping && mapping.any?
          raise DefinitionError, "#{self}: region #{mount.name.inspect}: on_final takes { Final => target }, got #{mapping.inspect}"
        end

        mapping.each do |final, target|
          final = mount.machine.send(:__handle, final, declare: false)
          raise DefinitionError, "#{self}: region #{mount.name.inspect} maps on_final #{final} twice" if mount.on_final.key?(final)

          mount.on_final[final] = __own_target(target)
        end
      end

      def __define_invoke(child, node:, payload:, on_done:)
        __check_child_machine!(child)
        raise DefinitionError, "#{self}: #{node.label} starts #{child} twice" if node.invokes.any? { |spec| spec.machine == child }

        on_done = on_done.each_with_object({}) do |(final, target), mapped|
          final = child.send(:__handle, final, declare: false)
          raise DefinitionError, "#{self}: start #{child} maps on_done #{final} twice" if mapped.key?(final)

          mapped[final] = __own_target(target)
        end
        spec = ChildSpec.new(machine: child, kind: :invoke, stop_on: NONE, node:, payload:, on_done:)
        node.invokes << spec
        spec
      end

      # A child a handler's +starts:+ names: one spec however many handlers start it.
      def __define_started(child)
        __check_child_machine!(child)
        children_specs[child] ||= ChildSpec.new(machine: child, kind: :spawn, stop_on: NONE, node: nil, payload: nil, on_done: {})
      end

      def __check_child_machine!(child)
        raise DefinitionError, "#{self}: #{child} is a part of #{child.owner}, not a machine to start" if Class === child && child < Part
        raise DefinitionError, "#{self}: #{child.inspect} isn't a Sourced::Machine" unless Class === child && child < Machine
        raise DefinitionError, "#{self}: #{child} has no start state" unless child.start_state
        raise DefinitionError, "#{self}: #{child} has no stopped state" unless child.stop_state
      end

      # A handler's +starts:+, as a list of machines (+nil+ among them: optional).
      # @param label [String] the handler, for errors
      # @return [Array<Class<Machine>, nil>]
      def __starts(starts, block, label)
        members =
          case starts
          in nil then return NONE
          in Starts then starts.members
          in Class if starts < Machine || starts < Part then [starts]
          else
            raise DefinitionError, "#{self}: #{label}: starts: takes a machine or a union of them (A | B, A | nil), got #{starts.inspect}"
          end
        members.compact.each { |child| __define_started(child) }
        if block.nil? && members.size > 1
          reason = members.include?(nil) ? 'nothing decides whether to start one' : 'nothing chooses which to start'
          raise DefinitionError, "#{self}: #{label} starts: #{starts} needs a block: #{reason}"
        end

        members
      end

      # An +always+ or +done+ of +node+, after +declared+ (its siblings so far).
      def __eventless(node, kind, target, guard, declared)
        unless guard in nil | Symbol | GuardInterface
          raise DefinitionError, "#{self}: #{node.label}'s #{kind}: if: takes a Symbol or a #call(context), got #{guard.inspect}"
        end

        eventless = Eventless.build(kind == 'done' ? __own_target(target) : __target(target), guard)
        if declared.any? { |other| !other.guarded? }
          raise DefinitionError, "#{self}: #{node.label}'s #{kind} #{eventless.target} is unreachable, after an unguarded #{kind}"
        end

        eventless
      end

      # One of this machine's states. A Symbol names one, declaring it
      # unless +declare+ is false.
      def __handle(target, declare: true)
        case target
        in Class if Machine.handle?(target) && target.machine == self then target
        in Class if Machine.handle?(target)
          raise DefinitionError, "#{self}: #{target} is #{target.machine}'s state, not #{self}'s"
        in Symbol | String if declare then __name_declared(State(target))
        in Symbol | String
          declared_states.fetch(target.to_sym) { raise DefinitionError, "#{self} has no state #{target.inspect}" }
        else
          raise DefinitionError, "#{self}: states are State handles or Symbols, got #{target.inspect}"
        end
      end

      # A state declared by Symbol has no constant of its own: set one
      # (+:awaiting_payment+ => +AwaitingPayment+), unless that name is taken.
      def __name_declared(handle)
        return handle if handle.name

        constant = handle.state_name.to_s.split('_').map(&:capitalize).join.to_sym
        # Left for stopped's messages: such a handle stays anonymous.
        return handle if CHILD_MESSAGES.include?(constant)

        const_set(constant, handle) unless const_defined?(constant, false)
        handle
      end

      # @param what [String] the macro declaring it, for errors: +command+ or +on+
      def __command_definition(declaration, block, what: 'command', starts: nil)
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
        starts = __starts(starts, block, "#{what} #{command_class}")
        if block.nil? && (targets.size > 1 || (targets == [nil] && starts.empty?))
          raise DefinitionError,
                "#{self}: #{command_class} => #{targets.map { |t| t || 'nil' }.join(' | ')} " \
                'needs a block to choose where to go'
        end

        CommandDef.new(command_class:, targets: targets.uniq, block:, starts:)
      end

      # @return [Array<State, nil>]
      def __flatten_targets(target)
        case target
        in nil then [nil]
        in Targets then target.members.map { |member| member && __target(member) }
        in Array then target.flat_map { |member| __flatten_targets(member) }
        else [__target(target)]
        end
      end

      # Where a transition may go: one of our states, or one of a mounted
      # part's, below the state that declares it (checked on validation),
      # or a {History} of one.
      def __target(target, declare: true)
        case target
        in History then target
        in Class if Machine.handle?(target) then target
        else __handle(target, declare:)
        end
      end

      # One of our states, or a {History} of one.
      def __own_target(target) = (target in History) ? target : __handle(target)
    end
  end
end
