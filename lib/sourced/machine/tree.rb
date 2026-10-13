# frozen_string_literal: true

module Sourced
  class Machine
    # A machine's definition placed into one tree: its regions and checkpoints,
    # each knowing where it sits. A {Region} or {Node} is a definition, owned by
    # the machine that declares it; a {PlacedRegion} or {PlacedNode} is that
    # definition at a position in the tree, with its own parent links. The
    # runtime, routing and rendering work on the placed tree.
    #
    # Placed objects are built once per tree and compared by identity: the
    # runtime keys positions by placed region, and compares placed nodes.
    class Tree
      attr_reader :root, :checkpoints, :regions, :all_nodes

      # @param machine [Class<Machine>]
      def initialize(machine)
        @placed = {}
        @checkpoints = {}
        @regions = {}
        @all_nodes = []
        @root = place_region(machine.root, :root, nil, machine)
      end

      # The placed node for a definition node, ex. an invoked child's checkpoint.
      # @param node [Node]
      # @return [PlacedNode]
      def placed(node) = @placed.fetch(node)

      # @param klass [Class]
      def checkpoint?(klass) = @checkpoints.key?(klass)

      private

      def place_region(region, name, parent, machine)
        placed = PlacedRegion.new(region, name, parent, machine)
        @regions[name] = placed unless parent.nil?
        region.nodes.each_value do |node|
          placed_node = PlacedNode.new(node, placed)
          @placed[node] = placed_node
          @all_nodes << placed_node
          @checkpoints[node.key] = placed_node unless node.initial?
          placed.nodes[node.key] = placed_node
          node.regions.each do |child|
            placed_node.regions << place_region(child, child.name, placed_node, machine)
          end
        end
        placed
      end
    end

    # A {Region} at its position in a {Tree}.
    class PlacedRegion
      attr_reader :region, :name, :parent, :machine, :nodes

      # @param region [Region] the definition
      # @param name [Symbol]
      # @param parent [PlacedNode, nil] nil for the tree's root
      # @param machine [Class<Machine>] the machine whose definition this is
      def initialize(region, name, parent, machine)
        @region = region
        @name = name
        @parent = parent
        @machine = machine
        @nodes = {}
      end

      def root? = parent.nil?

      # The region's position before its first checkpoint, declared or not.
      def initial_node = nodes[INITIAL] || (@blank_initial ||= PlacedNode.new(region.initial_node, self))

      # @return [Array<PlacedRegion>] this region and its ancestors, innermost first
      def lineage = root? ? [self] : [self, *parent.region.lineage]

      def label = root? ? 'the root region' : "region #{name.inspect}"
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
