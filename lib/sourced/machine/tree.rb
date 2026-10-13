# frozen_string_literal: true

module Sourced
  class Machine
    # A machine's definition placed into one tree: its regions and checkpoints,
    # and those of the machines it mounts as regions, each knowing where it
    # sits. A {Region} or {Node} is a definition, owned by the machine that
    # declares it; a {PlacedRegion} or {PlacedNode} is that definition at a
    # position in the tree, with its own parent links. The runtime, routing,
    # validation and rendering work on the placed tree.
    #
    # Placed objects are built once per tree and compared by identity: the
    # runtime keys positions by placed region, and compares placed nodes.
    class Tree
      attr_reader :root, :checkpoints, :regions, :all_nodes, :children_specs, :child_events

      # @param machine [Class<Machine>]
      # @raise [DefinitionError] if checkpoints, regions or children clash across machines
      def initialize(machine)
        @top = machine
        @placed = {}
        @checkpoints = {}
        @regions = {}
        @all_nodes = []
        @children_specs = {}
        @child_events = {}
        @owners = {}
        @machines = []
        @root = place_machine(machine, :root, nil, nil)
      end

      # The placed node for a definition node, ex. an invoked child's checkpoint.
      # @param node [Node]
      # @return [PlacedNode]
      def placed(node) = @placed.fetch(node)

      # @param klass [Class]
      def checkpoint?(klass) = @checkpoints.key?(klass)

      # @param klass [Class]
      def child_event?(klass) = @child_events.key?(klass)

      # @return [Array<Class<Machine>>] the machines mounted as regions, in tree order
      def mounted_machines = @machines.drop(1)

      # The machine that declares a child.
      # @param spec [ChildSpec]
      # @return [Class<Machine>]
      def owner(spec) = @owners.fetch(spec)

      private

      def place_machine(machine, name, parent, mount)
        if @machines.include?(machine)
          raise DefinitionError, "#{@top}: #{machine} is mounted twice; its checkpoints would be ambiguous"
        end

        @machines << machine
        machine.children_specs.each_value { |spec| add_child(machine, spec) }
        place_region(machine.root, name, parent, machine, mount)
      end

      def place_region(region, name, parent, machine, mount)
        placed = PlacedRegion.new(region, name, parent, machine, mount)
        unless parent.nil?
          raise DefinitionError, "#{@top}: region #{name.inspect} is declared twice" if @regions.key?(name)

          @regions[name] = placed
        end
        region.nodes.each_value { |node| place_node(node, placed, machine) }
        placed
      end

      def place_node(node, region, machine)
        placed = PlacedNode.new(node, region)
        @placed[node] = placed
        @all_nodes << placed
        unless node.initial?
          if @checkpoints.key?(node.key)
            raise DefinitionError, "#{@top}: checkpoint #{node.key} is declared twice (#{@checkpoints[node.key].label}, #{placed.label})"
          end

          @checkpoints[node.key] = placed
        end
        region.nodes[node.key] = placed
        node.regions.each do |child|
          placed.regions <<
            case child
            in RegionMount => mount then place_machine(mount.machine, mount.name, placed, mount)
            else place_region(child, child.name, placed, machine, nil)
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
        machine.child_events.each do |klass, entries|
          entries.each { |entry| (@child_events[klass] ||= []) << entry if entry.first == spec }
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

      # @return [Hash{Class => Class}] a mounted machine's root finals => the parent's checkpoints
      def on_done = mount ? mount.on_done : NONE_MAP

      # The region's position before its first checkpoint, declared or not.
      def initial_node = nodes[INITIAL] || (@blank_initial ||= PlacedNode.new(region.initial_node, self))

      # @return [Array<PlacedRegion>] this region and its ancestors, innermost first
      def lineage = root? ? [self] : [self, *parent.region.lineage]

      def label = root? ? 'the root region' : "region #{name.inspect}"

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
      def entries = node.entries
      def exits = node.exits
      def afters = node.afters
      def invokes = node.invokes
      def children_joins = node.children_joins
      def done = node.done
      def label = initial? ? "#{region.label}'s initial position" : key.to_s

      # Whether this checkpoint is inside +other+'s regions, at any depth.
      def descendant_of?(other)
        parent = region.parent
        !parent.nil? && (parent == other || parent.descendant_of?(other))
      end
    end
  end
end
