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
      # command, or where a compound state mounting this part starts. Given a
      # part, the start is that part's compound state ({.state}).
      # @param handle [State, Class<Part>, Symbol]
      def started(handle, *rest, &block)
        raise DefinitionError, "#{self}: started is declared twice" if @start_state

        __check_single_state!(handle, rest)
        @start_state = __handle(handle)
        state(@start_state, &block)
      end

      # The machine's position before its first state.
      def initial(&block) = __define_node(root, INITIAL, final: false, &block)

      # Place a state, and declare the commands it accepts.
      #
      # A part ({Part}) is its own compound state, +Part::State+: placing it
      # (by the part or its handle) mounts the part, whose states are its
      # sub-states, entered at the part's +started+ state:
      #
      #   state PlanningPart do
      #     on_final PlanningPart::Finalised => Reserving
      #   end
      #
      # The block declares the compound state's own commands and actions.
      # The part's states are this machine's, its context this machine's; it
      # escalates through its root finals, which +on_final+ maps onto our states.
      #
      # @param handle [State, Class<Part>, Symbol] one of this machine's states, or a part; a Symbol declares one
      def state(handle, *rest, &block)
        __check_single_state!(handle, rest)
        __define_node(root, handle, final: false, &block)
      end

      # Place a final state. It accepts no commands.
      # @param handle [State, Symbol]
      def final(handle, &block) = __define_node(root, handle, final: true, &block)

      private

      # A part mounts as its own compound state (+state Part+), not under another state.
      def __check_single_state!(handle, rest)
        return if rest.empty?

        raise DefinitionError,
              "#{self}: state #{handle.inspect}, #{rest.map(&:inspect).join(', ')}: a state takes no part; " \
              "a part is its own compound state, so mount it with state #{rest.first.inspect}"
      end

      def __define_node(region, key, final:, &block)
        unless key == INITIAL
          __check_mountable!("state #{key}", key) if Class === key && key < Machine
          key = __handle(key)
        end
        if region.nodes.key?(key) || nodes.key?(key)
          raise DefinitionError, "#{self}: #{key == INITIAL ? "#{region.label}'s initial position" : key} is placed twice"
        end

        node = Node.new(key, final:, region:)
        node.regions << __mount_part(key.part, final:) if key != INITIAL && key.part
        region.nodes[key] = node
        nodes[key] = node unless key == INITIAL
        NodeBuilder.new(self, node).instance_eval(&block) if block
        node
      end

      # A part's compound state mounts the part, named after the state.
      def __mount_part(part, final:)
        raise DefinitionError, "#{self}: final #{part}: a part is a compound state, and a final state can't be compound" if final

        __check_mountable!("state #{part}", part)
        raise DefinitionError, "#{self}: state #{part}: #{part} has no start state" unless part.start_state

        name = part.to_state_handle.state_name
        if (taken = mounts[name]) && taken.compound?
          raise DefinitionError,
                "#{self}: state #{part}: #{taken.machine}'s compound state is named #{name.inspect} too; give one of them another prefix:"
        end

        __mount(name, part, compound: true)
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
      # unless +declare+ is false. A part stands for its compound state,
      # which any machine of the part's owner's chart can place.
      def __handle(target, declare: true)
        target = Machine.state_handle(target)
        case target
        in Class if Machine.handle?(target)
          unless target.machine == self || target.part&.owner.equal?(__chart_owner)
            raise DefinitionError, "#{self}: #{target} is #{target.machine}'s state, not #{self}'s"
          end

          target
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
        declared, targets =
          case declaration
          in Hash if declaration.size == 1
            [declaration.keys.first, __flatten_targets(declaration.values.first)]
          in Hash
            raise DefinitionError, "#{self}: invalid #{what} declaration #{declaration.inspect}"
          else
            [declaration, [nil]]
          end
        command_class = declared.to_message_class
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
      # or a {History} of one. A part stands for its compound state.
      def __target(target, declare: true)
        target = Machine.state_handle(target)
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
