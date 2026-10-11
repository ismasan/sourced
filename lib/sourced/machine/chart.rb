# frozen_string_literal: true

module Sourced
  class Machine
    # A machine's definition as a JSON-serializable Hash, for rendering it. It
    # borrows XState's vocabulary and nesting (+states+, +initial+, +type+,
    # +on+, +entry+ / +exit+, +after+, +invoke+) and keeps Sourced's meaning
    # where the two differ:
    #
    # * A region's initial position is a node of its own (+type: 'initial'+,
    #   id +"<region>/initial"+), since it accepts commands. A machine with a
    #   +start+ checkpoint and no +initial+ block has none: it starts at +start+.
    # * A checkpoint's id is its message type. Its key in +states+ is the class
    #   name without modules (the full name, with +::+ as +_+, if that's taken).
    # * A checkpoint with regions is +type: 'parallel'+, its +states+ the regions.
    # * Each command is one transition, listing every checkpoint it may decide
    #   into as +targets+ (empty for a data-only command). Transitions are listed
    #   on the checkpoint that takes them, targets in any region.
    # * Joins and children use Sourced's names: +on_done+ (the +done+ join),
    #   +children_done+, +invoke+ (+on_done+ by child final), and the machine's
    #   +children+, +start+ and +stop+.
    # * +entry+ and +exit+ count the checkpoint's actions; +after+ lists its
    #   timers' delays in seconds.
    #
    # Every node, region and child has a +status+ key: +nil+ for a definition,
    # filled in for an instance (see {Machine#to_h}), so both render with the
    # same code:
    #
    # * a region: +{ active:, current:, done: }+, +current+ being the region's
    #   latest position (also for an inactive region: where it was left). The
    #   machine's (its root region's) also has +value+, as in {View#value}, with ids;
    # * a node: +{ active:, visited:, entered_by: }+. Visited means entered
    #   since its region was last entered, the current checkpoint included;
    #   +entered_by+ is the event that entered it, while it's active;
    # * a child: +{ instances: [{ keys:, state:, final: }] }+.
    #
    # The domain +context+ isn't included.
    class Chart
      # @param machine [Class<Sourced::Machine>] a validated machine
      # @param runtime [Runtime, nil] an instance's runtime, for its status
      def initialize(machine, runtime: nil)
        @machine = machine
        @runtime = runtime
      end

      # @return [Hash]
      def to_h
        region(@machine.root).merge(
          machine: @machine.name,
          partition_by: @machine.partition_keys.map(&:to_s),
          start: @machine.start_event&.type,
          stop: stop,
          children: @machine.children_specs.values.map { |spec| child(spec) },
          status: @runtime && region_status(@machine.root).merge(value: value(@machine.root))
        )
      end

      private

      def region(region)
        nodes = region.nodes.values.reject(&:initial?)
        states = {}
        states['initial'] = node(region.initial_node, 'initial') if initial_node?(region)
        names = nodes.map { |n| short_name(n.key) }
        nodes.each do |n|
          key = names.count(short_name(n.key)) > 1 ? n.key.name.gsub('::', '_') : short_name(n.key)
          states[key] = node(n, key)
        end

        {
          id: region.name.to_s,
          key: region.name.to_s,
          type: 'compound',
          initial: initial_node?(region) ? id(region.initial_node) : id(@machine.checkpoints.fetch(@machine.start_event)),
          states:,
          status: @runtime && region_status(region)
        }
      end

      def initial_node?(region) = !(region.root? && @machine.start_event && !region.nodes.key?(INITIAL))

      def node(node, key)
        {
          id: id(node),
          key:,
          type: node_type(node),
          message_type: node.initial? ? nil : node.key.type,
          class: node.initial? ? nil : node.key.name,
          entry: node.entries.size,
          exit: node.exits.size,
          after: node.afters.map { |timer| { delay: timer.delay } },
          on: node.commands.values.to_h { |definition| [definition.command_class.type, transition(definition)] },
          on_done: node.done && @machine.checkpoints.fetch(node.done).then { |target| id(target) },
          children_done: node.children_joins.map do |join|
            { as: join.as.to_s, targets: targets(join.definition.checkpoint_targets) }
          end,
          invoke: node.invokes.map do |spec|
            { id: spec.as.to_s, src: spec.machine.name, on_done: spec.on_done.to_h { |final, target| [final.type, target_id(target)] } }
          end,
          states: node.regions.to_h { |child| [child.name.to_s, region(child)] },
          status: @runtime && node_status(node)
        }
      end

      def node_type(node)
        if node.initial? then 'initial'
        elsif node.final? then 'final'
        elsif node.regions.any? then 'parallel'
        else 'atomic'
        end
      end

      def transition(definition)
        {
          event: definition.command_class.type,
          label: short_name(definition.command_class),
          targets: targets(definition.checkpoint_targets),
          optional: definition.optional?,
          auto: definition.auto?
        }
      end

      def stop
        return unless @machine.stop_event

        { event: @machine::Stop.type, label: 'Stop', targets: targets([@machine.stop_event]) }
      end

      def child(spec)
        {
          as: spec.as.to_s,
          machine: spec.machine.name,
          kind: spec.kind.to_s,
          checkpoint: spec.node && id(spec.node),
          stop_on: Array(spec.stop_on).map(&:type),
          status: @runtime && { instances: @runtime.children(spec.as).map { |ref| child_ref(ref) } }
        }
      end

      def child_ref(ref)
        { keys: ref.keys.to_h { |k, v| [k.to_s, v] }, state: ref.state.type, final: ref.final? }
      end

      def region_status(region)
        current = @runtime.current_node(region)
        { active: @runtime.active?(region), current: id(current), done: current.final? }
      end

      def node_status(node)
        active = @runtime.active_node?(node)
        entered_by = active && !node.initial? ? @runtime.entered_by(node.region) : nil
        {
          active:,
          visited: @runtime.visited?(node),
          entered_by: entered_by && { id: entered_by.id, type: entered_by.type, at: entered_by.created_at.utc.iso8601(6) }
        }
      end

      def value(region)
        current = @runtime.current_node(region)
        return id(current) if current.regions.empty?

        { id(current) => current.regions.to_h { |child| [child.name.to_s, value(child)] } }
      end

      def targets(classes) = classes.map { |klass| target_id(klass) }
      def target_id(klass) = id(@machine.checkpoints.fetch(klass))
      def id(node) = node.initial? ? "#{node.region.name}/initial" : node.key.type
      def short_name(klass) = klass.name.split('::').last
    end
  end
end
