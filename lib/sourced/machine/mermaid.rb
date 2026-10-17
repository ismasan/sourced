# frozen_string_literal: true

module Sourced
  class Machine
    # Draws a machine as a Mermaid state diagram (+stateDiagram-v2+):
    # states, regions as concurrent sections, and transitions labelled
    # with the commands (or joins) that take them, and +/ starts Child+ for
    # the children a handler's +starts:+ declares. Data-only handlers, which
    # don't move the machine, have no edge: those that start children, and the
    # children a state +stops+, are listed in a note on the state.
    # A note on a region's initial position goes on the composite state that
    # contains the region; the root's initial position, with no such state,
    # is drawn as an +initial+ state to carry it.
    #
    # State ids are state paths, +.+ as +_+. A transition to an ancestor
    # region's state starts from the state that takes it, and is declared
    # in the ancestor region. From a region's initial position, which has no
    # state of its own, it starts from the composite state that contains the
    # region. A child's +started+ state is entered from the diagram's
    # start. A compound state (a machine's) is a composite state entered
    # at its machine's start. A mounted machine's +on_done+ mappings are drawn
    # from its finals, labelled +on_done+. A transition to a state's
    # history goes to the state, its label suffixed +(H)+ (+(H*)+ for deep).
    #
    #   Sourced::Machine::Mermaid.new(OrderMachine).to_s
    class Mermaid
      # @param machine [Class<Sourced::Machine>] a validated machine
      def initialize(machine)
        @machine = machine
        @tree = machine.tree
        @edges = Hash.new { |h, k| h[k] = [] }
        @notes = Hash.new { |h, k| h[k] = [] }
      end

      # @return [String]
      def to_s
        @edges.clear
        @notes.clear
        collect_edges
        (['stateDiagram-v2'] + region_lines(@tree.root, '  ')).join("\n")
      end

      private

      def collect_edges
        @tree.all_nodes.each do |node|
          node.commands.each_value { |definition| handler(node, definition, short_name(definition.command_class)) }
          node.ons.each_value { |definition| handler(node, definition, "on #{short_name(definition.command_class)}") }
          @notes[note_node(node)] << "stops #{node.stops.map { short_name(it) }.join(', ')}" if node.stops.any?
          { 'always' => node.always, 'done' => node.dones }.each do |kind, list|
            list.each do |eventless|
              label = eventless.guarded? ? "#{kind} [#{eventless.guard_label}]" : kind
              edge(node, eventless.target, label)
            end
          end
          node.children_joins.each { |join| handler(node, join.definition, "children_done #{short_name(join.machine)}") }
          node.invokes.each do |spec|
            spec.on_done.each do |child_final, target|
              edge(node, target, "#{short_name(spec.machine)} #{child_final.state_name}")
            end
          end
        end
        # A mounted machine's root final escalates from the final itself.
        @tree.all_regions.each do |region|
          region.on_done.each { |final, target| edge(region.nodes.fetch(final), target, 'on_done', from: region.parent) }
        end
        return unless @machine.stop_state

        stop = @tree.root.nodes.fetch(@machine.stop_state)
        @tree.root.nodes.each_value do |node|
          edge(node, stop.key, 'Stop', from: stop) unless node.final? || node.initial?
        end
      end

      # A handler's edges, or a note for a data-only one that starts children.
      def handler(node, definition, label)
        if definition.starts.any?
          label = "#{label} / starts #{definition.starts.map { it ? short_name(it) : 'nil' }.join(' | ')}"
        end
        targets = definition.state_targets
        targets.each { |target| edge(node, target, label) }
        @notes[note_node(node)] << label if targets.empty? && definition.starts.any?
      end

      # The state a note on +node+ goes on: a region's initial position has
      # none, so the state containing the region, up to the root's.
      def note_node(node)
        node = node.region.parent while node.initial? && !node.region.root?
        node
      end

      # Whether +node+ is drawn as a state: states, and the root's
      # initial position when it carries a note.
      def drawn?(node) = !node.initial? || @notes.key?(node)

      def short_name(klass) = klass.name.split('::').last

      # Record a transition in the region it's drawn in: the target's, from the
      # state that takes it, or the state containing a region's initial position.
      # +target+ is resolved from +from+, the node declaring it.
      def edge(node, target, label, from: node)
        label = "#{label} #{target.deep ? '(H*)' : '(H)'}" if target in History
        target = @tree.resolve(from, target)
        source = node
        source = source.region.parent while source.initial? && !source.region.equal?(target.region)
        # Mermaid can't lay out a composite's edge to its own descendant
        # drawn inside the composite: draw it beside the composite instead.
        region = descendant?(target, source) ? source.region : target.region
        @edges[region] << [source, target, label]
      end

      def descendant?(node, ancestor)
        region = node.region
        until region.root?
          return true if region.parent.equal?(ancestor)

          region = region.parent.region
        end
        false
      end

      def region_lines(region, indent)
        lines = []
        region.nodes.each_value do |node|
          next unless drawn?(node)

          label = node.initial? ? 'initial' : node.key.state_name
          # Mermaid can't render an empty composite state: regions holding
          # only an initial position draw as a plain state.
          sections = node.regions.map { region_lines(it, "#{indent}  ") }.reject(&:empty?)
          if sections.empty?
            lines << %(#{indent}state "#{label}" as #{id(node)})
          else
            lines << %(#{indent}state "#{label}" as #{id(node)} {)
            sections.each_with_index do |section, i|
              lines << "#{indent}  --" if i.positive?
              lines.concat(section)
            end
            lines << "#{indent}}"
          end
        end
        lines << "#{indent}[*] --> #{id(region.nodes.fetch(@machine.start_state))}" if region.root? && @machine.start_state
        lines << "#{indent}[*] --> #{id(region.initial_node)}" if region.root? && drawn?(region.initial_node)
        lines << "#{indent}[*] --> #{id(region.start_node)}" if region.compound?
        @edges[region].uniq.each do |source, target, label|
          from = drawn?(source) ? id(source) : '[*]'
          lines << "#{indent}#{from} --> #{id(target)} : #{escape(label)}"
        end
        region.nodes.each_value do |node|
          lines << "#{indent}#{id(node)} --> [*]" if node.final?
        end
        region.nodes.each_value do |node|
          next unless @notes.key?(node)

          lines << "#{indent}note right of #{id(node)}"
          @notes[node].uniq.each { |note| lines << "#{indent}  #{note}" }
          lines << "#{indent}end note"
        end
        lines
      end

      def id(node) = node.path.tr('.', '_')

      # Mermaid reads +:+ in a transition label as syntax (+::+ fails to
      # parse), so as an entity.
      def escape(label) = label.gsub(':', '#58;')
    end
  end
end
