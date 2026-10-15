# frozen_string_literal: true

module Sourced
  class Machine
    # Draws a machine as a Mermaid state diagram (+stateDiagram-v2+):
    # checkpoints, regions as concurrent sections, and transitions labelled
    # with the commands (or joins) that take them. Data-only commands, which
    # don't move the machine, aren't drawn.
    #
    # State ids are checkpoint paths, +.+ as +_+. A transition to an ancestor
    # region's checkpoint starts from the state that takes it, and is declared
    # in the ancestor region. From a region's initial position, which has no
    # state of its own, it starts from the composite state that contains the
    # region. A child's +start+ checkpoint is entered from the diagram's
    # start. A compound checkpoint (a machine's) is a composite state entered
    # at its machine's start. A mounted machine's +on_done+ mappings are drawn
    # from its finals, labelled +on_done+. A transition to a checkpoint's
    # history goes to the checkpoint, its label suffixed +(H)+ (+(H*)+ for deep).
    #
    #   Sourced::Machine::Mermaid.new(OrderMachine).to_s
    class Mermaid
      # @param machine [Class<Sourced::Machine>] a validated machine
      def initialize(machine)
        @machine = machine
        @tree = machine.tree
        @edges = Hash.new { |h, k| h[k] = [] }
      end

      # @return [String]
      def to_s
        @edges.clear
        collect_edges
        (['stateDiagram-v2'] + region_lines(@tree.root, '  ')).join("\n")
      end

      private

      def collect_edges
        @tree.all_nodes.each do |node|
          node.commands.each_value do |definition|
            label = definition.command_class.name.split('::').last
            definition.checkpoint_targets.each { |target| edge(node, target, label) }
          end
          node.ons.each_value do |definition|
            label = "on #{definition.command_class.name.split('::').last}"
            definition.checkpoint_targets.each { |target| edge(node, target, label) }
          end
          { 'always' => node.always, 'done' => node.dones }.each do |kind, list|
            list.each do |eventless|
              label = eventless.guarded? ? "#{kind} [#{eventless.guard_label}]" : kind
              edge(node, eventless.target, label)
            end
          end
          node.children_joins.each do |join|
            join.definition.checkpoint_targets.each { |target| edge(node, target, "children_done #{join.as}") }
          end
          node.invokes.each do |spec|
            spec.on_done.each do |child_final, target|
              edge(node, target, "#{spec.as} #{child_final.checkpoint_name}")
            end
          end
        end
        # A mounted machine's root final escalates from the final itself.
        @tree.all_regions.each do |region|
          region.on_done.each { |final, target| edge(region.nodes.fetch(final), target, 'on_done', from: region.parent) }
        end
        return unless @machine.stop_checkpoint

        stop = @tree.root.nodes.fetch(@machine.stop_checkpoint)
        @tree.root.nodes.each_value do |node|
          edge(node, stop.key, 'Stop', from: stop) unless node.final? || node.initial?
        end
      end

      # Record a transition in the region it's drawn in: the target's, from the
      # state that takes it, or the state containing a region's initial position.
      # +target+ is resolved from +from+, the node declaring it.
      def edge(node, target, label, from: node)
        label = "#{label} #{target.deep ? '(H*)' : '(H)'}" if target in History
        target = @tree.resolve(from, target)
        source = node
        source = source.region.parent while source.initial? && !source.region.equal?(target.region)
        @edges[target.region] << [source, target, label]
      end

      def region_lines(region, indent)
        lines = []
        region.nodes.each_value do |node|
          next if node.initial?

          label = node.key.checkpoint_name
          if node.regions.empty?
            lines << %(#{indent}state "#{label}" as #{id(node)})
          else
            lines << %(#{indent}state "#{label}" as #{id(node)} {)
            node.regions.each_with_index do |child, i|
              lines << "#{indent}  --" if i.positive?
              lines.concat(region_lines(child, "#{indent}  "))
            end
            lines << "#{indent}}"
          end
        end
        lines << "#{indent}[*] --> #{id(region.nodes.fetch(@machine.start_checkpoint))}" if region.root? && @machine.start_checkpoint
        lines << "#{indent}[*] --> #{id(region.start_node)}" if region.compound?
        @edges[region].uniq.each do |source, target, label|
          from = source.initial? ? '[*]' : id(source)
          lines << "#{indent}#{from} --> #{id(target)} : #{label}"
        end
        region.nodes.each_value do |node|
          lines << "#{indent}#{id(node)} --> [*]" if node.final?
        end
        lines
      end

      def id(node) = node.path.tr('.', '_')
    end
  end
end
