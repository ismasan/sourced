# frozen_string_literal: true

module Sourced
  class Machine
    # Draws a machine as a Mermaid state diagram (+stateDiagram-v2+):
    # checkpoints, regions as concurrent sections, and transitions labelled
    # with the commands (or joins) that take them. Data-only commands, which
    # don't move the machine, aren't drawn.
    #
    # A transition to an ancestor region's checkpoint starts from the state
    # that takes it, and is declared in the ancestor region. From a region's
    # initial position, which has no state of its own, it starts from the
    # composite state that contains the region. A child's +start+ checkpoint
    # is entered from the diagram's start. A compound checkpoint (a machine's)
    # is a composite state entered at its machine's start, drawn as
    # +<id>__start+ since it's the same event. A mounted machine's +on_done+
    # mappings are drawn from its finals, labelled +on_done+.
    #
    #   Sourced::Machine::Mermaid.new(OrderMachine).to_s
    class Mermaid
      # @param machine [Class<Sourced::Machine>] a validated machine
      def initialize(machine)
        @machine = machine
        @edges = Hash.new { |h, k| h[k] = [] }
      end

      # @return [String]
      def to_s
        @edges.clear
        collect_edges
        (['stateDiagram-v2'] + region_lines(@machine.tree.root, '  ')).join("\n")
      end

      private

      def collect_edges
        @machine.all_nodes.each do |node|
          node.commands.each_value do |definition|
            label = short_name(definition.command_class)
            definition.checkpoint_targets.each { |target| edge(node, target, label) }
          end
          edge(node, node.done, 'done') if node.done
          node.children_joins.each do |join|
            join.definition.checkpoint_targets.each { |target| edge(node, target, "children_done #{join.as}") }
          end
          node.invokes.each do |spec|
            spec.on_done.each { |child_final, target| edge(node, target, "#{spec.as} #{short_name(child_final)}") }
          end
        end
        # A mounted machine's root final escalates from the final itself.
        @machine.tree.regions.each_value do |region|
          region.on_done.each { |final, target| edge(region.nodes.fetch(final), target, 'on_done') }
        end
        return unless @machine.stop_event

        @machine.tree.root.nodes.each_value do |node|
          edge(node, @machine.stop_event, 'Stop') unless node.final? || node.initial?
        end
      end

      # Record a transition in the region it's drawn in: the target's, from the
      # state that takes it, or the state containing a region's initial position.
      def edge(node, target, label)
        target_node = @machine.tree.checkpoints.fetch(target)
        source = node
        source = source.region.parent while source.initial? && source.region != target_node.region
        @edges[target_node.region] << [source, target_node, label]
      end

      def region_lines(region, indent)
        lines = []
        region.nodes.each_value do |node|
          next if node.initial?

          if node.regions.empty?
            lines << %(#{indent}state "#{short_name(node.key)}" as #{node_id(node)})
          else
            lines << %(#{indent}state "#{short_name(node.key)}" as #{node_id(node)} {)
            node.regions.each_with_index do |child, i|
              lines << "#{indent}  --" if i.positive?
              lines.concat(region_lines(child, "#{indent}  "))
            end
            lines << "#{indent}}"
          end
        end
        lines << "#{indent}[*] --> #{id(@machine.start_event)}" if region.root? && @machine.start_event
        lines << "#{indent}[*] --> #{node_id(region.nodes.fetch(region.start))}" if region.compound?
        @edges[region].uniq.each do |source, target, label|
          from = source.initial? ? '[*]' : node_id(source)
          lines << "#{indent}#{from} --> #{node_id(target)} : #{label}"
        end
        region.nodes.each_value do |node|
          lines << "#{indent}#{node_id(node)} --> [*]" if node.final?
        end
        lines
      end

      def id(klass) = klass.name.gsub('::', '_')
      def node_id(node) = node.compound_start? ? "#{id(node.key)}__start" : id(node.key)
      def short_name(klass) = klass.name.split('::').last
    end
  end
end
