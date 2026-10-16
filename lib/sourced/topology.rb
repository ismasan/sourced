# frozen_string_literal: true

begin
  require 'prism'
rescue LoadError
  # Prism not available; SourceAnalyzer will return empty produces arrays.
end

module Sourced
  # Analyzes registered reactors (Deciders, Projectors and Machines) and builds a
  # flat array of node structs describing the message flow graph. Enables
  # visualization, introspection, and Event Modeling diagram generation.
  module Topology
    CommandNode = Struct.new(:type, :id, :name, :group_id, :group_label, :produces, :schema, keyword_init: true)
    EventNode = Struct.new(:type, :id, :name, :group_id, :group_label, :produces, :schema, keyword_init: true)
    AutomationNode = Struct.new(:type, :id, :name, :group_id, :group_label, :consumes, :produces, keyword_init: true)
    ReadModelNode = Struct.new(:type, :id, :name, :group_id, :group_label, :consumes, :produces, :schema, keyword_init: true)

    # Analyze registered reactors and build the topology graph.
    #
    # @param reactors [Enumerable<Class>] reactor classes (Deciders and/or Projectors)
    # @return [Array<CommandNode, EventNode, AutomationNode, ReadModelNode>]
    def self.build(reactors)
      analyzer = SourceAnalyzer.new
      nodes = []
      command_ids = {}
      event_nodes = {}

      reactors.each do |reactor|
        group_id = reactor.group_id
        group_label = reactor.group_label

        if reactor.is_a?(Class) && reactor < Sourced::Machine
          add_machine(reactor, analyzer, nodes, command_ids, event_nodes)
        elsif reactor.respond_to?(:handled_commands)
          reactor.handled_commands.each do |cmd_class|
            next if command_ids.key?(cmd_class.type)

            produced_refs = analyzer.events_produced_by(reactor, cmd_class)
            produced_types = resolve_refs(produced_refs, reactor)

            schema = extract_schema(cmd_class)
            cmd_node = CommandNode.new(
              type: 'command',
              id: cmd_class.type,
              name: cmd_class.name,
              group_id: group_id,
              group_label: group_label,
              produces: produced_types,
              schema: schema
            )
            nodes << cmd_node
            command_ids[cmd_class.type] = cmd_node

            produced_types.each do |evt_type|
              next if event_nodes.key?(evt_type)
              next if command_ids.key?(evt_type)

              evt_class = find_event_class(evt_type)
              next unless evt_class

              event_nodes[evt_type] = EventNode.new(
                type: 'event',
                id: evt_type,
                name: evt_class.name,
                group_id: group_id,
                group_label: group_label,
                produces: [],
                schema: extract_schema(evt_class)
              )
            end
          end
        end

        if reactor.respond_to?(:handled_messages_for_evolve)
          reactor.handled_messages_for_evolve.each do |evt_class|
            next if evt_class < Sourced::Command

            evt_type = evt_class.type
            next if event_nodes.key?(evt_type)
            next if command_ids.key?(evt_type)

            event_nodes[evt_type] = EventNode.new(
              type: 'event',
              id: evt_type,
              name: evt_class.name,
              group_id: group_id,
              group_label: group_label,
              produces: [],
              schema: extract_schema(evt_class)
            )
          end
        end

        is_projector = reactor < Sourced::Projector
        rm_id = is_projector ? "#{Sourced::Types::ModuleToMessageType.parse(group_id)}-rm" : nil
        projector_aut_ids = []

        if reactor.respond_to?(:handled_messages_for_react)
          catch_all_events = reactor.respond_to?(:catch_all_react_events) ? reactor.catch_all_react_events : Set.new
          specific_events = reactor.handled_messages_for_react.reject { |e| catch_all_events.include?(e) }

          specific_events.each do |evt_class|
            produced_refs = analyzer.commands_dispatched_by(reactor, evt_class)
            produced_types = resolve_refs(produced_refs, reactor)

            aut_id = "#{evt_class.type}-#{group_id}-aut"
            projector_aut_ids << aut_id if is_projector

            nodes << AutomationNode.new(
              type: 'automation',
              id: aut_id,
              name: "reaction(#{evt_class.name})",
              group_id: group_id,
              group_label: group_label,
              consumes: is_projector ? [rm_id] : [evt_class.type],
              produces: produced_types
            )
          end

          if catch_all_events.any?
            produced_refs = analyzer.commands_dispatched_by(reactor, catch_all_events.first)
            produced_types = resolve_refs(produced_refs, reactor)

            group_type_id = Sourced::Types::ModuleToMessageType.parse(group_id)
            aut_id = "#{group_type_id}-aut"
            projector_aut_ids << aut_id if is_projector

            consumes = if is_projector
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

        if is_projector
          consumes = reactor.handled_messages_for_evolve
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

    # A machine's commands produce the classes of the checkpoints they move
    # to (see {Machine.checkpoint_class}), the +Started+ of the children their
    # +starts:+ declare, and what their blocks emit (+event+). Entry, exit and
    # +after+ actions, joins, and starting and stopping children are automations.
    # What runs on entering a checkpoint consumes its class; what runs on
    # leaving it (exit actions, stopping the children it started) consumes the
    # classes of the transitions that can leave it.
    # @api private
    def self.add_machine(machine, analyzer, nodes, command_ids, event_nodes)
      machine.validate!
      group_id = machine.group_id
      type_of = ->(handle) { machine.checkpoint_class(handle).type }
      produced = Hash.new { |h, k| h[k] = [] }

      machine.all_nodes.each do |node|
        node.commands.each_value do |definition|
          produced[definition.command_class] |= machine_block_produces(machine, analyzer, definition)
        end
      end
      produced[machine::Stop] |= [type_of.call(machine.stop_checkpoint), machine::Finished.type] if machine.stop_checkpoint

      produced.each do |cmd_class, types|
        if (existing = command_ids[cmd_class.type])
          existing.produces |= types
        else
          cmd_node = CommandNode.new(
            type: 'command', id: cmd_class.type, name: cmd_class.name, group_id:,
            produces: types, schema: extract_schema(cmd_class)
          )
          nodes << cmd_node
          command_ids[cmd_class.type] = cmd_node
        end
        types.each { |type| add_event_node(event_nodes, command_ids, find_event_class(type), group_id) }
      end

      automation = lambda do |id, name, consumes, produces|
        nodes << AutomationNode.new(type: 'automation', id: "#{id}-#{group_id}-aut", name:, group_id:, consumes:, produces:)
      end

      dispatched_by = lambda do |blocks|
        blocks.flat_map { |blk| resolve_refs(analyzer.calls_in_block(blk, :dispatch), machine) }.uniq
      end
      transitions = machine_transitions(machine)

      machine.all_nodes.each do |node|
        # Initial positions take events too.
        node.ons.each_value do |definition|
          event_class = definition.command_class
          automation.call("#{node.path}-on-#{event_class.type}", "on(#{node.path}, #{event_class.name.split('::').last})",
                          [event_class.type], machine_block_produces(machine, analyzer, definition))
        end

        next if node.initial?

        entering = [type_of.call(node.key)]
        leaving = transitions.select { |source, target| exits?(source, target, node) }.map { |_, target| type_of.call(target.key) }.uniq
        entry_dispatched = dispatched_by.call(node.entries + node.afters.map(&:block))
        automation.call(node.path, "actions(#{node.path})", entering, entry_dispatched) if entry_dispatched.any?
        exit_dispatched = dispatched_by.call(node.exits)
        automation.call("#{node.path}-exit", "exit(#{node.path})", leaving, exit_dispatched) if exit_dispatched.any?

        if node.dones.any?
          finals = node.regions.flat_map { |region| region.nodes.each_value.select(&:final?).map { type_of.call(it.key) } }
          joined = eventless_closure(machine, node.dones.map(&:target))
          automation.call("#{node.path}-done", "done(#{node.path})", finals.uniq, joined.map(&type_of))
        end

        node.children_joins.each do |join|
          child = join.machine
          automation.call("#{node.path}-children-done-#{child.group_id}", "children_done(#{child.group_id})", [child::Finished.type],
                          machine_block_produces(machine, analyzer, join.definition))
        end

        node.invokes.each do |spec|
          automation.call("#{node.path}-start-#{spec.machine.group_id}", "start(#{spec.machine.name})",
                          entering, [spec.machine::Started.type])
          automation.call("#{node.path}-stop-#{spec.machine.group_id}", "stop(#{spec.machine.name})",
                          leaving, [spec.machine::Stop.type])
          if spec.on_done.any?
            automation.call("#{node.path}-on-done-#{spec.machine.group_id}", "on_done(#{spec.machine.group_id})", [spec.machine::Finished.type],
                            eventless_closure(machine, spec.on_done.values.uniq).map(&type_of))
          end
        end
      end

      machine.tree.children_specs.each_value do |spec|
        next if spec.invoke? || spec.stop_on.empty?

        automation.call("stop-#{spec.machine.group_id}", "stop(#{spec.machine.name})",
                        spec.stop_on.map { |node| type_of.call(node.key) }.uniq, [spec.machine::Stop.type])
      end
    end

    # Every transition the definition declares, as +[source, target]+ placed
    # nodes: commands, +children_done+, +always+, +done+, checkpoint-started and
    # mounted machines' +on_done+, and +stopped+ (from each top-level checkpoint).
    # @api private
    def self.machine_transitions(machine)
      tree = machine.tree
      pairs = tree.all_nodes.flat_map do |node|
        definitions = node.commands.values + node.ons.values + node.children_joins.map(&:definition)
        targets = definitions.flat_map(&:checkpoint_targets)
        targets += (node.always + node.dones).map(&:target)
        targets += node.invokes.flat_map { |spec| spec.on_done.values }
        own = targets.map { |handle| [node, tree.resolve(node, handle)] }
        mounted = node.regions.flat_map do |region|
          region.on_done.map { |final, handle| [region.nodes.fetch(final), tree.resolve(node, handle)] }
        end
        own + mounted
      end
      if machine.stop_checkpoint
        stop = tree.root.nodes.fetch(machine.stop_checkpoint)
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

    # @api private
    def self.machine_block_produces(machine, analyzer, definition)
      reached = eventless_closure(machine, definition.checkpoint_targets)
      types = reached.map { |handle| machine.checkpoint_class(handle).type }
      # Reaching a root final finishes a child machine.
      if machine.__finished_class && reached.any? { |handle| machine.root.nodes[handle]&.final? }
        types << machine.__finished_class.type
      end
      types += resolve_refs(analyzer.calls_in_block(definition.block, :event), machine)
      types += definition.started_machines.map { |child| child::Started.type }
      types.uniq
    end

    # +handles+, and every checkpoint their +always+ or a mounted machine's
    # +on_done+ can move on to, through the checkpoints entering them enters
    # (compound starts), each once.
    # @api private
    def self.eventless_closure(machine, handles)
      tree = machine.tree
      reached = []
      pending = handles.map(&:checkpoint)
      until pending.empty?
        handle = pending.shift
        next if reached.include?(handle)

        reached << handle
        tree.placements(handle).flat_map { |placed| tree.default_entry(placed) }.each do |entered|
          pending.concat(entered.always.map { |eventless| tree.resolve(entered, eventless.target).key })
          # A mounted machine's mapped final escalates in the same decision.
          mapped = entered.region.on_done[entered.key]
          pending << tree.resolve(entered.region.parent, mapped).key if mapped
        end
      end
      reached
    end

    # @api private
    def self.add_event_node(event_nodes, command_ids, evt_class, group_id)
      return unless evt_class
      return if event_nodes.key?(evt_class.type) || command_ids.key?(evt_class.type)

      event_nodes[evt_class.type] = EventNode.new(
        type: 'event', id: evt_class.type, name: evt_class.name, group_id:,
        produces: [], schema: extract_schema(evt_class)
      )
    end

    # @api private
    def self.resolve_refs(refs, reactor)
      refs.filter_map do |ref_type, ref_value|
        resolve_ref(ref_type, ref_value, reactor)
      end.uniq
    end

    # @api private
    def self.resolve_ref(ref_type, ref_value, reactor)
      case ref_type
      when :symbol
        resolve_symbol_ref(ref_value, reactor)
      when :const
        resolve_const_ref(ref_value, reactor)
      when :const_path
        resolve_const_path_ref(ref_value)
      when :const_index
        resolve_const_index_ref(ref_value, reactor)
      end
    end

    def self.resolve_symbol_ref(symbol_name, reactor)
      sym = symbol_name.to_sym
      if reactor.respond_to?(:[])
        begin
          reactor[sym]&.type
        rescue StandardError
          nil
        end
      end
    end

    def self.resolve_const_ref(const_name, reactor)
      klass = resolve_constant_in_context(const_name, reactor)
      klass&.respond_to?(:type) ? klass.type : nil
    end

    def self.resolve_const_path_ref(path)
      klass = Object.const_get(path)
      klass.type
    rescue NameError
      nil
    end

    def self.resolve_const_index_ref(ref, reactor)
      receiver_name = ref[:receiver]
      klass = if receiver_name.include?('::')
                Object.const_get(receiver_name)
              else
                resolve_constant_in_context(receiver_name, reactor)
              end
      return nil unless klass && klass.respond_to?(:[])

      klass[ref[:index].to_sym].type
    rescue StandardError
      nil
    end

    def self.resolve_constant_in_context(const_name, reactor)
      if reactor.const_defined?(const_name, false)
        return reactor.const_get(const_name, false)
      end

      parts = reactor.name.split('::')
      while parts.length > 1
        parts.pop
        begin
          mod = Object.const_get(parts.join('::'))
          if mod.const_defined?(const_name, false)
            return mod.const_get(const_name, false)
          end
        rescue NameError
          break
        end
      end

      Object.const_get(const_name) rescue nil
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

    private_class_method :add_machine, :machine_block_produces, :machine_transitions, :exits?, :branch, :eventless_closure, :add_event_node,
                         :resolve_refs, :resolve_ref, :resolve_symbol_ref,
                         :resolve_const_ref, :resolve_const_path_ref,
                         :resolve_const_index_ref,
                         :resolve_constant_in_context, :find_event_class,
                         :extract_schema

    # Prism-based source analyzer for extracting produced events/commands from handler blocks.
    class SourceAnalyzer
      def initialize
        @file_cache = {}
        @prism_available = check_prism
      end

      def events_produced_by(reactor, cmd_class)
        return [] unless @prism_available

        method_name = Sourced.message_method_name(Decider::PREFIX, cmd_class.name)
        extract_calls_from_handler(reactor, method_name, :event)
      end

      def commands_dispatched_by(reactor, evt_class)
        return [] unless @prism_available

        method_name = Sourced.message_method_name(React::PREFIX, evt_class.name)
        extract_calls_from_handler(reactor, method_name, :dispatch)
      end

      # Calls to +call_name+ inside a block (ex. a machine's command handler).
      # @param block [Proc, nil]
      def calls_in_block(block, call_name)
        return [] unless @prism_available && block

        calls_at(*block.source_location, call_name)
      end

      private

      def check_prism
        defined?(::Prism)
      end

      def parse_file(path)
        @file_cache[path] ||= Prism.parse_file(path)
      end

      def extract_calls_from_handler(reactor, method_name, call_name)
        calls_at(*reactor.instance_method(method_name).source_location, call_name)
      rescue NameError
        []
      end

      def calls_at(file, line, call_name)
        return [] unless file && line && File.exist?(file)

        block = find_block_at_line(parse_file(file).value, line)
        block ? collect_calls(block, call_name) : []
      end

      def find_block_at_line(program, target_line)
        finder = BlockAtLineFinder.new(target_line)
        program.accept(finder)
        finder.found_block
      end

      def collect_calls(block_node, target_name)
        collector = CallCollector.new(target_name)
        block_node.accept(collector)
        collector.refs
      end

      if defined?(::Prism)
        class BlockAtLineFinder < Prism::Visitor
          attr_reader :found_block

          def initialize(target_line)
            @target_line = target_line
            @found_block = nil
          end

          def visit_call_node(node)
            if node.block && (node.location.start_line == @target_line || node.block.location.start_line == @target_line)
              @found_block = node.block
            end
            super
          end
        end

        class CallCollector < Prism::Visitor
          attr_reader :refs

          def initialize(target_name)
            @target_name = target_name
            @refs = []
          end

          def visit_call_node(node)
            target = find_target_call(node)
            if target
              extract_first_arg(target)
            end
            super
          end

          private

          def find_target_call(node)
            return node if node.name == @target_name

            if node.receiver.is_a?(Prism::CallNode)
              find_target_call(node.receiver)
            end
          end

          def extract_first_arg(call_node)
            return unless call_node.arguments

            arg = call_node.arguments.arguments.first
            ref = case arg
                  when Prism::SymbolNode
                    [:symbol, arg.unescaped]
                  when Prism::ConstantReadNode
                    [:const, arg.name.to_s]
                  when Prism::ConstantPathNode
                    [:const_path, constant_path_to_string(arg)]
                  when Prism::CallNode
                    extract_const_index(arg)
                  end
            @refs << ref if ref
          end

          def extract_const_index(call_node)
            return unless call_node.name == :[]
            return unless call_node.arguments&.arguments&.size == 1

            sym_arg = call_node.arguments.arguments.first
            return unless sym_arg.is_a?(Prism::SymbolNode)

            receiver = call_node.receiver
            receiver_name = case receiver
                            when Prism::ConstantReadNode
                              receiver.name.to_s
                            when Prism::ConstantPathNode
                              constant_path_to_string(receiver)
                            end
            return unless receiver_name

            [:const_index, { receiver: receiver_name, index: sym_arg.unescaped }]
          end

          def constant_path_to_string(node)
            parts = []
            current = node
            while current.is_a?(Prism::ConstantPathNode)
              parts.unshift(current.name.to_s)
              current = current.parent
            end
            parts.unshift(current.name.to_s) if current.is_a?(Prism::ConstantReadNode)
            parts.join('::')
          end
        end
      end
    end
  end
end
