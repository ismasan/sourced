# frozen_string_literal: true

module Sourced
  # Analyzes registered reactors (Deciders, Projectors and Machines) and builds a
  # flat array of node structs describing the message flow graph. Enables
  # visualization, introspection, and Event Modeling diagram generation.
  module Topology
    CommandNode = Struct.new(:type, :id, :name, :group_id, :group_label, :produces, :schema, keyword_init: true)
    # +transition+: a machine's state event, appended as it moves into a state.
    EventNode = Struct.new(:type, :id, :name, :group_id, :group_label, :produces, :schema, :transition, keyword_init: true)
    # +state+: for a machine's automation, the path of the state it belongs to.
    AutomationNode = Struct.new(:type, :id, :name, :group_id, :group_label, :consumes, :produces, :state, keyword_init: true)
    ReadModelNode = Struct.new(:type, :id, :name, :group_id, :group_label, :consumes, :produces, :schema, keyword_init: true)

    # The topology of a set of reactors, built on its first read and kept:
    # building walks every reactor's definition, and most processes never read it.
    # Concurrent first reads build it once. The graph is frozen, nodes and
    # their lists included, since every reader shares it.
    class Memo
      # @param reactors [Enumerable<Class>]
      def initialize(reactors)
        @reactors = reactors
        @lock = Mutex.new
        @value = nil
      end

      # @return [Array<CommandNode, EventNode, AutomationNode, ReadModelNode>]
      def value
        @value || @lock.synchronize do
          @value ||= Topology.build(@reactors).each do |node|
            node.each { |field| field.freeze if Array === field }
            node.freeze
          end.freeze
        end
      end
    end

    # Analyze registered reactors and build the topology graph.
    #
    # @param reactors [Enumerable<Class>] reactor classes (Deciders and/or Projectors)
    # @return [Array<CommandNode, EventNode, AutomationNode, ReadModelNode>]
    def self.build(reactors)
      nodes = []
      command_ids = {}
      event_nodes = {}

      reactors.each do |reactor|
        group_id = reactor.group_id
        group_label = reactor.group_label

        if reactor.is_a?(Class) && reactor < Sourced::Machine
          add_machine(reactor, nodes, command_ids, event_nodes)
        elsif reactor.respond_to?(:command_outputs)
          reactor.command_outputs.each do |cmd_class, outputs|
            next if command_ids.key?(cmd_class.type)

            cmd_node = CommandNode.new(
              type: 'command',
              id: cmd_class.type,
              name: cmd_class.name,
              group_id: group_id,
              group_label: group_label,
              produces: outputs.messages.map(&:type),
              schema: extract_schema(cmd_class)
            )
            nodes << cmd_node
            command_ids[cmd_class.type] = cmd_node

            outputs.messages.each do |evt_class|
              add_event_node(event_nodes, command_ids, evt_class, group_id, group_label)
            end
          end
        end

        evolved(reactor).each do |evt_class|
          add_event_node(event_nodes, command_ids, evt_class, group_id, group_label) unless evt_class < Sourced::Command
        end

        is_read_model = reactor < Sourced::Projector
        rm_id = is_read_model ? "#{Sourced::Types::ModuleToMessageType.parse(group_id)}-rm" : nil
        projector_aut_ids = []

        if reactor.respond_to?(:handled_messages_for_react)
          catch_all_events = reactor.respond_to?(:catch_all_react_events) ? reactor.catch_all_react_events : Set.new
          specific_events = reactor.handled_messages_for_react.reject { |e| catch_all_events.include?(e) }

          specific_events.each do |evt_class|
            produced_types = reaction_produces(reactor, evt_class)

            aut_id = "#{evt_class.type}-#{group_id}-aut"
            projector_aut_ids << aut_id if is_read_model

            nodes << AutomationNode.new(
              type: 'automation',
              id: aut_id,
              name: "reaction(#{evt_class.name})",
              group_id: group_id,
              group_label: group_label,
              consumes: is_read_model ? [rm_id] : [evt_class.type],
              produces: produced_types
            )
          end

          if catch_all_events.any?
            produced_types = reaction_produces(reactor, catch_all_events.first)

            group_type_id = Sourced::Types::ModuleToMessageType.parse(group_id)
            aut_id = "#{group_type_id}-aut"
            projector_aut_ids << aut_id if is_read_model

            consumes = if is_read_model
              [rm_id]
            else
              catch_all_events.map(&:type)
            end

            nodes << AutomationNode.new(
              type: 'automation',
              id: aut_id,
              name: "reaction(#{group_label})",
              group_id: group_id,
              group_label: group_label,
              consumes: consumes,
              produces: produced_types
            )
          end
        end

        if is_read_model
          consumes = evolved(reactor)
            .reject { |c| c < Sourced::Command }
            .map(&:type)

          nodes << ReadModelNode.new(
            type: 'readmodel',
            id: rm_id,
            name: group_label,
            group_id: group_id,
            group_label: group_label,
            consumes: consumes,
            produces: projector_aut_ids,
            schema: {}
          )
        end
      end

      nodes + event_nodes.values
    end

    # A machine's commands (its parts' too) produce the states they move
    # to (a part's states are its owner's) and the +Started+ of the children their
    # +starts:+ declare; their data events aren't edges. Entry, exit and
    # +after+ actions (producing what they declare), joins, and starting and
    # stopping children are automations.
    # What runs on entering a state consumes its class; what runs on
    # leaving it (exit actions, stopping the children it started) consumes the
    # classes of the transitions that can leave it.
    # @api private
    def self.add_machine(machine, nodes, command_ids, event_nodes)
      machine.validate!
      group_id = machine.group_id
      group_label = machine.group_label
      produced = Hash.new { |h, k| h[k] = [] }

      machine.all_nodes.each do |node|
        node.commands.each_value do |definition|
          produced[definition.command_class] |= handler_produces(machine, definition)
        end
      end
      produced[machine::Stop] |= [machine.stop_state.type, machine::Finished.type] if machine.stop_state

      produced.each do |cmd_class, types|
        if (existing = command_ids[cmd_class.type])
          existing.produces |= types
        else
          cmd_node = CommandNode.new(
            type: 'command', id: cmd_class.type, name: cmd_class.name, group_id:, group_label:,
            produces: types, schema: extract_schema(cmd_class)
          )
          nodes << cmd_node
          command_ids[cmd_class.type] = cmd_node
        end
        types.each { |type| add_event_node(event_nodes, command_ids, find_event_class(type), group_id, group_label) }
      end

      automation = lambda do |state, id, name, consumes, produces|
        nodes << AutomationNode.new(type: 'automation', id: "#{id}-#{group_id}-aut", name:, group_id:, group_label:, consumes:, produces:, state:)
      end

      dispatched_by = ->(actions) { actions.flat_map { |action| action.outputs.messages.map(&:type) } }
      transitions = machine_transitions(machine)

      machine.all_nodes.each do |node|
        # Initial positions take events too.
        node.ons.each_value do |definition|
          event_class = definition.command_class
          automation.call(node.path, "#{node.path}-on-#{event_class.type}", "on(#{node.path}, #{event_class.name.split('::').last})",
                          [event_class.type], handler_produces(machine, definition))
        end

        next if node.initial?

        entering = [node.key.type]
        leaving = transitions.select { |source, target| exits?(source, target, node) }.map { |_, target| target.key.type }.uniq
        timed = node.afters.flat_map { |timer| timer.commands.map(&:type) }
        entry_dispatched = (dispatched_by.call(node.entries) + timed).uniq
        automation.call(node.path, node.path, "actions(#{node.path})", entering, entry_dispatched) if entry_dispatched.any?
        exit_dispatched = dispatched_by.call(node.exits).uniq
        automation.call(node.path, "#{node.path}-exit", "exit(#{node.path})", leaving, exit_dispatched) if exit_dispatched.any?

        if node.dones.any?
          finals = node.regions.flat_map { |region| region.nodes.each_value.select(&:final?).map { it.key.type } }
          joined = eventless_closure(machine, node.dones.map(&:target))
          automation.call(node.path, "#{node.path}-done", "done(#{node.path})", finals.uniq, joined.map(&:type))
        end

        node.children_joins.each do |join|
          child = join.machine
          automation.call(node.path, "#{node.path}-children-done-#{child.group_id}", "children_done(#{child.name})", [child::Finished.type],
                          handler_produces(machine, join.definition))
        end

        node.invokes.each do |spec|
          automation.call(node.path, "#{node.path}-start-#{spec.machine.group_id}", "start(#{spec.machine.name})",
                          entering, [spec.machine::Started.type])
          automation.call(node.path, "#{node.path}-stop-#{spec.machine.group_id}", "stop(#{spec.machine.name})",
                          leaving, [spec.machine::Stop.type])
          if spec.on_done.any?
            automation.call(node.path, "#{node.path}-on-done-#{spec.machine.group_id}", "on_done(#{spec.machine.name})", [spec.machine::Finished.type],
                            eventless_closure(machine, spec.on_done.values.uniq).map(&:type))
          end
        end
      end

      machine.tree.children_specs.each do |spec|
        next if spec.invoke? || spec.stop_on.empty?

        automation.call(nil, "stop-#{spec.machine.group_id}", "stop(#{spec.machine.name})",
                        spec.stop_on.map { |node| node.key.type }.uniq, [spec.machine::Stop.type])
      end
    end

    # Every transition the definition declares, as +[source, target]+ placed
    # nodes: commands, +children_done+, +always+, +done+, state-started and
    # mounted parts' +on_final+, and +stopped+ (from each top-level state).
    # @api private
    def self.machine_transitions(machine)
      tree = machine.tree
      pairs = tree.all_nodes.flat_map do |node|
        definitions = node.commands.values + node.ons.values + node.children_joins.map(&:definition)
        targets = definitions.flat_map(&:state_targets)
        targets += (node.always + node.dones).map(&:target)
        targets += node.invokes.flat_map { |spec| spec.on_done.values }
        own = targets.map { |handle| [node, tree.resolve(node, handle)] }
        mounted = node.regions.flat_map do |region|
          region.on_final.map { |final, handle| [region.nodes.fetch(final), tree.resolve(node, handle)] }
        end
        own + mounted
      end
      if machine.stop_state
        stop = tree.root.nodes.fetch(machine.stop_state)
        pairs += tree.root.nodes.each_value.reject { |node| node.equal?(stop) }.map { |node| [node, stop] }
      end
      pairs
    end

    # Whether a transition from +source+ to +target+ can leave +node+, by the
    # runtime's rule ({Machine::Runtime#transition_sets}): it leaves what is
    # active below the innermost region holding both, on +source+'s side.
    # @api private
    def self.exits?(source, target, node)
      lineage = target.region.lineage
      domain = source.regions.find { |r| lineage.include?(r) } || source.region.lineage.find { |r| lineage.include?(r) }
      return false unless domain && node.region.lineage.include?(domain)

      # Active together with +source+: the same branch where their regions meet.
      meet = node.region.lineage.find { |r| source.region.lineage.include?(r) }
      branch(node, meet).equal?(branch(source, meet))
    end

    # +node+, or its ancestor, placed in +region+.
    # @api private
    def self.branch(node, region)
      node = node.region.parent until node.region.equal?(region)
      node
    end

    # What a handler appends: the states it moves to (and on, through the
    # +always+ closure), its machine's +Finished+ when one is a root final, and
    # the +Started+ of the children it declares. Its data events aren't edges.
    # @api private
    def self.handler_produces(machine, definition)
      reached = eventless_closure(machine, definition.state_targets)
      types = reached.map(&:type)
      # Reaching a root final finishes a child machine.
      if machine.__finished_class && reached.any? { |handle| machine.root.nodes[handle]&.final? }
        types << machine.__finished_class.type
      end
      types += definition.started_machines.map { |child| child::Started.type }
      types.uniq
    end

    # +handles+, and every state their +always+ or a mounted part's
    # +on_final+ can move on to, through the states entering them enters
    # (compound starts), each once.
    # @api private
    def self.eventless_closure(machine, handles)
      tree = machine.tree
      reached = []
      pending = handles.map(&:state)
      until pending.empty?
        handle = pending.shift
        next if reached.include?(handle)

        reached << handle
        tree.placements(handle).flat_map { |placed| tree.default_entry(placed) }.each do |entered|
          pending.concat(entered.always.map { |eventless| tree.resolve(entered, eventless.target).key })
          # A mounted part's mapped final escalates in the same decision.
          mapped = entered.region.on_final[entered.key]
          pending << tree.resolve(entered.region.parent, mapped).key if mapped
        end
      end
      reached
    end

    # What +reactor+ evolves from, however it's partitioned: its
    # +evolve_selector+'s types, or its history selector's.
    # @api private
    def self.evolved(reactor)
      (reactor.respond_to?(:evolve_selector) ? reactor.evolve_selector : reactor.history_selector).types
    end

    # The types a reaction declares it appends (none for a subscription
    # without a handler).
    # @api private
    def self.reaction_produces(reactor, evt_class)
      outputs = reactor.reaction_outputs[evt_class]
      outputs ? outputs.messages.map(&:type) : []
    end

    # @api private
    def self.add_event_node(event_nodes, command_ids, evt_class, group_id, group_label)
      return unless evt_class
      return if event_nodes.key?(evt_class.type) || command_ids.key?(evt_class.type)

      event_nodes[evt_class.type] = EventNode.new(
        type: 'event', id: evt_class.type, name: evt_class.name, group_id:, group_label:,
        produces: [], schema: extract_schema(evt_class), transition: evt_class < Sourced::Machine::State
      )
    end

    def self.find_event_class(type_string)
      Sourced::Message.registry[type_string]
    end

    def self.extract_schema(msg_class)
      return {} unless msg_class.const_defined?(:Payload, false)

      payload_class = msg_class::Payload
      if payload_class.respond_to?(:to_json_schema)
        payload_class.to_json_schema
      else
        {}
      end
    rescue StandardError
      {}
    end

    private_class_method :evolved, :reaction_produces, :add_machine, :handler_produces, :machine_transitions, :exits?,
                         :branch, :eventless_closure, :add_event_node, :find_event_class, :extract_schema
  end
end
