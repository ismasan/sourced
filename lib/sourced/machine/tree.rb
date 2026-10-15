# frozen_string_literal: true

module Sourced
  class Machine
    # A machine's definition placed into one tree: its regions and checkpoints,
    # and those of the machines it mounts, each knowing where it sits. A
    # {Region} or {Node} is a definition, owned by the machine that declares
    # it; a {PlacedRegion} or {PlacedNode} is that definition at a position in
    # the tree, with its own parent links and path. A machine mounted twice is
    # placed twice: its checkpoints are one set of handles at two sets of paths.
    # The runtime, routing, validation and rendering work on the placed tree.
    #
    # Placed objects are built once per tree and compared by identity: the
    # runtime keys positions by placed region, and compares placed nodes.
    class Tree
      attr_reader :root, :all_nodes, :all_regions, :children_specs, :child_events

      # @param machine [Class<Machine>]
      # @raise [DefinitionError] if regions or children clash across machines
      def initialize(machine)
        @top = machine
        @placed = {}
        @placements = {}
        @paths = {}
        @region_paths = {}
        @all_nodes = []
        @all_regions = []
        @children_specs = {}
        @child_events = {}
        @owners = {}
        @machines = []
        @resolved = {}
        @root = place_machine(machine, :root, nil, nil)
      end

      # The placed nodes for a definition node: more than one when its machine is
      # mounted more than once.
      # @param node [Node]
      # @return [Array<PlacedNode>]
      def placed(node) = @placed.fetch(node)

      # @param handle [Checkpoint]
      # @return [Array<PlacedNode>] where the checkpoint is placed
      def placements(handle) = @placements.fetch(handle, NONE)

      # @param handle [Checkpoint]
      def checkpoint?(handle) = @placements.key?(handle)

      # @return [Array<Checkpoint>] every placed checkpoint
      def checkpoints = @placements.keys

      # @param path [String] a checkpoint's path, or a region's initial position's (+<region path>.initial+)
      # @param message [Sourced::Message, nil] the stored message +path+ comes from, named in the error
      # @return [PlacedNode]
      # @raise [UnresolvablePathError] if nothing is at +path+
      def node_at(path, message = nil)
        @paths.fetch(path) do
          region_path, _, last = path.rpartition('.')
          region = last == INITIAL.to_s && @region_paths[region_path.empty? ? nil : region_path]
          unless region
            from = message && " (#{message.type} #{message.id})"
            raise UnresolvablePathError, "#{@top}: no checkpoint at #{path.inspect}#{from}"
          end

          region.initial_node
        end
      end

      # @param klass [Class]
      def child_event?(klass) = @child_events.key?(klass)

      # @return [Array<Class<Machine>>] the machines mounted, in tree order, once each
      def mounted_machines = @machines.drop(1).uniq

      # The machine that declares a child.
      # @param spec [ChildSpec]
      # @return [Class<Machine>]
      def owner(spec) = @owners.fetch(spec)

      # Where a transition declared on +node+ to +handle+ goes: the placement of
      # +handle+ in +node+'s region or the nearest ancestor region that has one,
      # or else the one below +node+. Never a sibling region's.
      #
      # @param node [PlacedNode] the node that declares the transition
      # @param target [Checkpoint, History] a {History} resolves to its checkpoint
      # @return [PlacedNode]
      # @raise [DefinitionError] if +target+ isn't reachable from +node+, is
      #   below it more than once, or is the history of a checkpoint without regions
      def resolve(node, target)
        @resolved.fetch([node, target]) do
          placed = resolve!(node, target.checkpoint)
          if (target in History) && placed.regions.empty?
            raise DefinitionError, "#{@top}: #{node.label} targets #{target}, but #{placed.label} has no regions to remember"
          end

          @resolved[[node, target]] = placed
        end
      end

      # What entering +node+ from outside enters: +node+, then its default
      # configuration, each compound checkpoint's machine's start all the way
      # down (any other region starts at its initial position, which isn't entered).
      # @param node [PlacedNode]
      # @return [Array<PlacedNode>] outermost first
      def default_entry(node)
        [node, *node.regions.flat_map { |region| region.compound? ? default_entry(region.start_node) : NONE }]
      end

      # @param handle [Checkpoint] a checkpoint of the root region
      # @return [Array<String>] the paths starting at +handle+ enters
      def default_entry_paths(handle) = default_entry(root.nodes.fetch(handle)).map(&:path)

      private

      def resolve!(node, handle)
        placements = placements(handle)
        raise DefinitionError, "#{@top}: #{node.label} targets #{handle.inspect}, which isn't a checkpoint" if placements.empty?

        lineage = node.region.lineage
        around = placements.select { |placed| lineage.include?(placed.region) }
        return around.max_by { |placed| placed.region.depth } if around.any?

        below = placements.select { |placed| placed.descendant_of?(node) }
        if below.size > 1
          raise DefinitionError,
                "#{@top}: #{node.label} targets #{handle.inspect}, which is below it #{below.size} times " \
                "(#{below.map(&:path).join(', ')})"
        end
        return below.first if below.any?

        raise DefinitionError,
              "#{@top}: #{node.label} targets #{handle.inspect} (at #{placements.map(&:path).join(', ')}); " \
              "#{node.label} can only reach #{node.region.label}, its ancestors, or its own regions"
      end

      def place_machine(machine, name, parent, mount)
        if @machines.include?(machine) && machine.children_specs.any?
          raise DefinitionError, "#{@top}: #{machine} is mounted twice, and declares children, which would be shared"
        end

        @machines << machine
        machine.children_specs.each_value { |spec| add_child(machine, spec) }
        place_region(machine.root, name, parent, machine, mount)
      end

      def place_region(region, name, parent, machine, mount)
        placed = PlacedRegion.new(region, name, parent, machine, mount)
        @all_regions << placed
        @region_paths[placed.path] = placed unless placed.compound?
        region.nodes.each_value do |node|
          # A compound checkpoint's machine starts at its start checkpoint: its
          # initial position is never reached, so it isn't part of the tree.
          next if node.initial? && placed.compound?

          place_node(node, placed)
        end
        placed
      end

      def place_node(node, region)
        placed = PlacedNode.new(node, region)
        (@placed[node] ||= []) << placed
        @all_nodes << placed
        unless node.initial?
          (@placements[node.key] ||= []) << placed
          if @paths.key?(placed.path)
            raise DefinitionError, "#{@top}: two checkpoints at #{placed.path} (#{@paths[placed.path].node.key.inspect}, #{node.key.inspect})"
          end

          @paths[placed.path] = placed
        end
        region.nodes[node.key] = placed
        node.regions.each do |child|
          placed.regions <<
            case child
            in RegionMount => mount then place_machine(mount.machine, mount.name, placed, mount)
            else place_region(child, child.name, placed, region.machine, nil)
            end
        end
      end

      def add_child(machine, spec)
        if @children_specs.key?(spec.as)
          raise DefinitionError, "#{@top}: child #{spec.as.inspect} is declared twice (#{@owners[@children_specs[spec.as]]}, #{machine})"
        end
        if @children_specs.each_value.any? { |known| known.machine == spec.machine }
          raise DefinitionError, "#{@top}: #{spec.machine} is already a child as another name"
        end

        @children_specs[spec.as] = spec
        @owners[spec] = machine
        [spec.machine::Started, spec.machine::Finished].each do |klass|
          (@child_events[klass] ||= []) << spec
        end
      end
    end

    # A {Region} at its position in a {Tree}.
    class PlacedRegion
      attr_reader :region, :name, :parent, :machine, :mount, :nodes

      # @param region [Region] the definition
      # @param name [Symbol]
      # @param parent [PlacedNode, nil] nil for the tree's root
      # @param machine [Class<Machine>] the machine whose definition this is
      # @param mount [RegionMount, nil] when the region is a mounted machine's root
      def initialize(region, name, parent, machine, mount)
        @region = region
        @name = name
        @parent = parent
        @machine = machine
        @mount = mount
        @nodes = {}
      end

      def root? = parent.nil?

      # A compound checkpoint's region: entered at its machine's start
      # checkpoint, with the checkpoint that mounts it.
      def compound? = mount&.compound? || false

      # @return [PlacedNode, nil] where a compound region starts
      def start_node = compound? ? nodes.fetch(machine.start_checkpoint) : nil

      # @return [Hash{Checkpoint => Checkpoint}] a mounted machine's root finals => the parent's checkpoints
      def on_done = mount ? mount.on_done : NONE_MAP

      # The region's position before its first checkpoint, declared or not.
      def initial_node = nodes[INITIAL] || (@blank_initial ||= PlacedNode.new(region.initial_node, self))

      # @return [Array<PlacedRegion>] this region and its ancestors, innermost first
      def lineage = @lineage ||= root? ? [self] : [self, *parent.region.lineage]

      def depth = lineage.size

      # The region's part of its checkpoints' paths: a compound region's
      # checkpoints follow its checkpoint directly, a parallel region's go
      # through the region's name.
      # @return [String, nil] nil for the root
      def path
        return @path if defined?(@path)

        @path = if root? then nil
                elsif compound? then parent.path
                else "#{parent.path}.#{name}"
                end
      end

      def label = root? ? 'the root region' : "region #{path}"

      NONE_MAP = {}.freeze
    end

    # A {Node} at its position in a {Tree}: the definition's commands and
    # actions, the tree's structure.
    class PlacedNode
      attr_reader :node, :region, :regions

      # @param node [Node] the definition
      # @param region [PlacedRegion]
      def initialize(node, region)
        @node = node
        @region = region
        @regions = []
      end

      # @return [Class<Machine>] the machine whose definition this is
      def machine = region.machine

      def key = node.key
      def final? = node.final?
      def initial? = node.initial?
      def commands = node.commands
      def ons = node.ons
      def entries = node.entries
      def exits = node.exits
      def afters = node.afters
      def invokes = node.invokes
      def children_joins = node.children_joins
      def always = node.always
      def dones = node.dones
      def transient? = node.transient?

      # Dot-separated names from the root: +reserving.suppliers.hotel.holding+.
      # A region's initial position is +<region path>.initial+.
      def path = @path ||= [region.path, initial? ? INITIAL : key.checkpoint_name].compact.join('.')

      def label = initial? ? "#{region.label}'s initial position" : path

      # @return [PlacedRegion, nil] the region of a compound checkpoint
      def compound_region = regions.find(&:compound?)

      # Whether this checkpoint is inside +other+'s regions, at any depth.
      def descendant_of?(other)
        parent = region.parent
        !parent.nil? && (parent.equal?(other) || parent.descendant_of?(other))
      end
    end
  end
end
