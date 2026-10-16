# frozen_string_literal: true

module Sourced
  class Machine
    # A machine's definition as a JSON-serializable Hash, for rendering it. It
    # borrows XState's vocabulary and nesting (+states+, +initial+, +type+,
    # +on+, +entry+ / +exit+, +after+, +invoke+) and keeps Sourced's meaning
    # where the two differ:
    #
    # * A checkpoint's id is its path (+reserving.suppliers.hotel.holding+),
    #   its key in +states+ its name.
    # * A region's initial position is a node of its own (+type: 'initial'+,
    #   id +"<region path>.initial"+), since it accepts commands. A machine with
    #   a +started+ checkpoint and no +initial+ block has none: it starts there.
    # * A checkpoint with regions is +type: 'parallel'+, its +states+ the regions.
    # * A compound checkpoint (a machine's, see {Machine.checkpoint}) is
    #   +type: 'compound'+, its +states+ the machine's checkpoints, its +initial+
    #   the machine's start.
    # * Each command is one transition, listing every checkpoint it may move
    #   to as +targets+ (empty for a data-only command). Transitions are listed
    #   on the checkpoint that takes them, targets in any region.
    # * Joins and children use Sourced's names: +on_done+ (the +done+ join),
    #   +children_done+, +invoke+ (+on_done+ by child final), and the machine's
    #   +children+, +start+ and +stop+. +mounts+ lists the machines a checkpoint
    #   mounts (as regions, or as a compound checkpoint's), with +on_done+ by
    #   their root final.
    # * +entry+ and +exit+ count the checkpoint's actions; +after+ lists its
    #   timers' delays in seconds.
    # * A transition to a checkpoint's history ({Checkpoint.history}) targets a
    #   history node in its +states+, as XState's: +type: 'history'+,
    #   +history: 'shallow' | 'deep'+, key +$history+ / +$deep_history+. It's
    #   only there when something targets it.
    #
    # Every node, region and child has a +status+ key: +nil+ for a definition,
    # filled in for an instance (see {Machine#to_h}), so both render with the
    # same code:
    #
    # * a region: +{ active:, current:, done: }+, +current+ being the region's
    #   latest position (also for an inactive region: where it was left; +nil+
    #   for a child machine that hasn't started). The
    #   machine's (its root region's) also has +value+, as in {View#value}, with ids;
    # * a node: +{ active:, visited:, entered_by: }+. Visited means entered
    #   since its region was last entered, the current checkpoint included;
    #   +entered_by+ is the transition that entered it, while it's active;
    # * a child: +{ instances: [{ keys:, state:, final: }] }+.
    #
    # The domain +context+ isn't included.
    class Chart
      # @param machine [Class<Sourced::Machine>] a validated machine
      # @param runtime [Runtime, nil] an instance's runtime, for its status
      def initialize(machine, runtime: nil)
        @machine = machine
        @runtime = runtime
        @tree = machine.tree
        @histories = history_targets
      end

      # @return [Hash]
      def to_h
        region(@tree.root).merge(
          machine: @machine.name,
          partition_by: @machine.partition_keys.map(&:to_s),
          start: @machine.start_checkpoint && root_id(@machine.start_checkpoint),
          stop: stop,
          children: @tree.children_specs.values.map { |spec| child(spec) },
          status: @runtime && region_status(@tree.root).merge(value: value(@tree.root))
        )
      end

      private

      def region(region)
        states = {}
        states['initial'] = node(region.initial_node) if initial_node?(region)
        region.nodes.each_value { |n| states[n.key.checkpoint_name.to_s] = node(n) unless n.initial? }

        {
          id: region.path || 'root',
          key: region.name.to_s,
          type: 'compound',
          initial: (initial_node?(region) ? region.initial_node : start_node(region)).path,
          states:,
          status: @runtime && region_status(region)
        }
      end

      def initial_node?(region)
        !region.compound? && !(region.root? && @machine.start_checkpoint && !region.nodes.key?(INITIAL))
      end

      # Where a region without an initial position starts: a compound
      # checkpoint's machine's start, or the machine's own +started+ checkpoint.
      def start_node(region) = region.compound? ? region.start_node : region.nodes.fetch(@machine.start_checkpoint)

      def node(node)
        compound = node.compound_region
        {
          id: node.path,
          key: node.initial? ? 'initial' : node.key.checkpoint_name.to_s,
          type: node_type(node),
          machine: node.machine.name,
          entry: node.entries.size,
          exit: node.exits.size,
          after: node.afters.map { |timer| { delay: timer.delay } },
          on: transitions(node, node.commands, 'command').merge(transitions(node, node.ons, 'event')),
          on_done: on_done_joins(node),
          always: eventless(node, node.always),
          children_done: node.children_joins.map do |join|
            { as: join.as.to_s, targets: targets(node, join.definition.checkpoint_targets) }
          end,
          invoke: node.invokes.map do |spec|
            { id: spec.as.to_s, src: spec.machine.name, on_done: on_done(node, spec.machine, spec.on_done) }
          end,
          mounts: node.regions.select(&:mount).map do |region|
            { region: region.name.to_s, src: region.machine.name, on_done: on_done(node, region.machine, region.on_done, region) }
          end,
          states: (compound ? region(compound)[:states] : node.regions.to_h { |child| [child.name.to_s, region(child)] })
            .merge(history_nodes(node)),
          status: @runtime && node_status(node)
        }.tap { |hash| hash[:initial] = compound.start_node.path if compound }
      end

      # +{ target:, guard: }+ each, +guard+ only when there is one.
      def eventless(node, list)
        list.map do |eventless|
          { target: target_id(node, eventless.target), guard: eventless.guard_label }.compact
        end
      end

      # A single unguarded +done+ is its target's id, as XState's +onDone+;
      # guarded ones a list, as {#eventless}.
      def on_done_joins(node)
        case node.dones
        in [] then nil
        in [eventless] unless eventless.guarded? then target_id(node, eventless.target)
        else eventless(node, node.dones)
        end
      end

      def node_type(node)
        if node.initial? then 'initial'
        elsif node.final? then 'final'
        elsif node.compound_region then 'compound'
        elsif node.regions.any? then 'parallel'
        else 'atomic'
        end
      end

      # Commands and +on+ events, by type: +kind+ tells them apart.
      def transitions(node, definitions, kind)
        definitions.values.to_h { |definition| [definition.command_class.type, transition(node, definition, kind)] }
      end

      def transition(node, definition, kind)
        {
          kind:,
          event: definition.command_class.type,
          label: definition.command_class.name.split('::').last,
          targets: targets(node, definition.checkpoint_targets),
          optional: definition.optional?,
          auto: definition.auto?
        }
      end

      def stop
        return unless @machine.stop_checkpoint

        { event: @machine::Stop.type, label: 'Stop', targets: [root_id(@machine.stop_checkpoint)] }
      end

      def child(spec)
        {
          as: spec.as.to_s,
          machine: spec.machine.name,
          kind: spec.kind.to_s,
          checkpoint: spec.node && @tree.placed(spec.node).first.path,
          stop_on: spec.stop_on.map { |handle| @tree.placements(handle).first.path },
          status: @runtime && { instances: @runtime.children(spec.as).map { |ref| child_ref(ref) } }
        }
      end

      def child_ref(ref)
        { keys: ref.keys.to_h { |k, v| [k.to_s, v] }, state: ref.state.checkpoint_name.to_s, final: ref.final? }
      end

      def region_status(region)
        current = @runtime.current_node(region)
        { active: @runtime.active?(region), current: position_id(current), done: current.final? }
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
        return position_id(current) if current.regions.empty?
        return { current.path => value(current.compound_region) } if current.compound_region

        { current.path => current.regions.to_h { |child| [child.name.to_s, value(child)] } }
      end

      # A child machine that hasn't started has no node for its position.
      def position_id(node) = node.initial? && !initial_node?(node.region) ? nil : node.path

      # A mounted machine's or child's finals, by name, => the paths they map to.
      def on_done(node, machine, map, region = nil)
        map.to_h do |final, target|
          from = region ? region.nodes.fetch(final).path : final.checkpoint_name.to_s
          [from, target_id(node, target)]
        end
      end

      def targets(node, handles) = handles.map { |handle| target_id(node, handle) }
      def root_id(handle) = @tree.root.nodes.fetch(handle).path

      # A checkpoint's id, or its history node's.
      def target_id(node, target)
        path = @tree.resolve(node, target).path
        (target in History) ? "#{path}.#{history_key(target)}" : path
      end

      def history_key(history) = history.deep ? '$deep_history' : '$history'

      def history_nodes(node)
        @histories.fetch(node.path, NONE).to_h do |history|
          key = history_key(history)
          [key, { id: "#{node.path}.#{key}", key:, type: 'history', history: history.deep ? 'deep' : 'shallow' }]
        end
      end

      # The histories something targets, by their checkpoint's path.
      def history_targets
        @tree.all_nodes.each_with_object(Hash.new { |h, k| h[k] = [] }) do |node, found|
          definitions = node.commands.values + node.ons.values + node.children_joins.map(&:definition)
          targets = definitions.flat_map(&:checkpoint_targets) + (node.always + node.dones).map(&:target) +
                    node.invokes.flat_map { |spec| spec.on_done.values } + node.regions.flat_map { |region| region.on_done.values }
          targets.grep(History).each do |history|
            list = found[@tree.resolve(node, history).path]
            list << history unless list.include?(history)
          end
        end
      end
    end
  end
end
