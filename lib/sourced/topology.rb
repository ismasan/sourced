# frozen_string_literal: true

begin
  require 'prism'
rescue LoadError
  # Prism not available; SourceAnalyzer will return empty produces arrays.
end

module Sourced
  module Topology
    CommandNode = Struct.new(:type, :id, :name, :group_id, :produces, :schema, keyword_init: true)
    EventNode = Struct.new(:type, :id, :name, :group_id, :produces, :schema, keyword_init: true)
    AutomationNode = Struct.new(:type, :id, :name, :group_id, :consumes, :produces, keyword_init: true)

    # Analyze registered reactors and build the topology graph.
    # @param reactors [Enumerable<Class>] reactor classes (Actors, Projectors)
    # @return [Array<CommandNode, EventNode, AutomationNode>]
    def self.build(reactors)
      analyzer = SourceAnalyzer.new
      nodes = []
      command_ids = {} # type_string => CommandNode (dedup across reactors)
      event_nodes = {} # type_string => EventNode (dedup)

      reactors.each do |reactor|
        group_id = reactor.consumer_info.group_id

        # Command nodes (actors only)
        if reactor.respond_to?(:handled_commands)
          reactor.handled_commands.each do |cmd_class|
            next if command_ids.key?(cmd_class.type)

            produced_refs = analyzer.events_produced_by(reactor, cmd_class)
            produced_types = resolve_refs(produced_refs, reactor, :event)

            schema = extract_schema(cmd_class)
            cmd_node = CommandNode.new(
              type: 'command',
              id: cmd_class.type,
              name: cmd_class.name,
              group_id: group_id,
              produces: produced_types,
              schema: schema
            )
            nodes << cmd_node
            command_ids[cmd_class.type] = cmd_node

            # Register event nodes from produces
            produced_types.each do |evt_type|
              next if event_nodes.key?(evt_type)
              next if command_ids.key?(evt_type)

              evt_class = find_event_class(evt_type, reactor)
              next unless evt_class

              event_nodes[evt_type] = EventNode.new(
                type: 'event',
                id: evt_type,
                name: evt_class.name,
                group_id: group_id,
                produces: [],
                schema: extract_schema(evt_class)
              )
            end
          end
        end

        # Event nodes from evolve handlers (covers projectors and events not yet seen)
        if reactor.respond_to?(:handled_messages_for_evolve)
          reactor.handled_messages_for_evolve.each do |evt_class|
            # Skip command classes that ended up in evolve handlers
            next if evt_class < Sourced::Command

            evt_type = evt_class.type
            next if event_nodes.key?(evt_type)
            next if command_ids.key?(evt_type)

            event_nodes[evt_type] = EventNode.new(
              type: 'event',
              id: evt_type,
              name: evt_class.name,
              group_id: group_id,
              produces: [],
              schema: extract_schema(evt_class)
            )
          end
        end

        # Automation nodes from reactions
        if reactor.respond_to?(:handled_messages_for_react)
          reactor.handled_messages_for_react.each do |evt_class|
            produced_refs = analyzer.commands_dispatched_by(reactor, evt_class)
            produced_types = resolve_refs(produced_refs, reactor, :command)

            nodes << AutomationNode.new(
              type: 'automation',
              id: "#{evt_class.type}-#{group_id}-aut",
              name: "reaction(#{evt_class.name})",
              group_id: group_id,
              consumes: [evt_class.type],
              produces: produced_types
            )
          end
        end
      end

      nodes + event_nodes.values
    end

    # Resolve AST references to message type strings.
    # @param refs [Array<Array>] e.g. [[:const, "ThingCreated"], [:symbol, "notify"]]
    # @param reactor [Class] reactor class for namespace resolution
    # @param kind [Symbol] :event or :command
    # @return [Array<String>] resolved type strings
    def self.resolve_refs(refs, reactor, kind)
      refs.filter_map do |ref_type, ref_value|
        resolve_ref(ref_type, ref_value, reactor, kind)
      end.uniq
    end

    # @api private
    def self.resolve_ref(ref_type, ref_value, reactor, kind)
      case ref_type
      when :symbol
        resolve_symbol_ref(ref_value, reactor, kind)
      when :const
        resolve_const_ref(ref_value, reactor)
      when :const_path
        resolve_const_path_ref(ref_value)
      end
    end

    def self.resolve_symbol_ref(symbol_name, reactor, kind)
      sym = symbol_name.to_sym
      if kind == :event && reactor.respond_to?(:resolve_message_class)
        begin
          reactor.resolve_message_class(sym).type
        rescue StandardError
          nil
        end
      elsif reactor.respond_to?(:[])
        begin
          reactor[sym].type
        rescue StandardError
          nil
        end
      end
    end

    def self.resolve_const_ref(const_name, reactor)
      # Search in reactor namespace, then enclosing modules, then top-level
      klass = resolve_constant_in_context(const_name, reactor)
      klass&.type
    end

    def self.resolve_const_path_ref(path)
      klass = Object.const_get(path)
      klass.type
    rescue NameError
      nil
    end

    # Try to find a constant by unqualified name within the reactor's module hierarchy.
    def self.resolve_constant_in_context(const_name, reactor)
      # 1. Check reactor's own constants
      if reactor.const_defined?(const_name, false)
        return reactor.const_get(const_name, false)
      end

      # 2. Walk enclosing modules
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

      # 3. Top-level
      Object.const_get(const_name) rescue nil
    end

    def self.find_event_class(type_string, reactor)
      # Try reactor's event registry first
      if reactor.respond_to?(:const_get) && reactor.const_defined?(:Event, false)
        klass = reactor::Event.registry[type_string]
        return klass if klass
      end

      # Fall back to global Event registry
      Sourced::Event.registry[type_string]
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

    private_class_method :resolve_refs, :resolve_ref, :resolve_symbol_ref,
                         :resolve_const_ref, :resolve_const_path_ref,
                         :resolve_constant_in_context, :find_event_class,
                         :extract_schema

    # Prism-based source analyzer for extracting produced events/commands from handler blocks.
    class SourceAnalyzer
      def initialize
        @file_cache = {}
        @prism_available = check_prism
      end

      # Extract event references from a command handler block.
      # @param reactor [Class]
      # @param cmd_class [Class]
      # @return [Array<Array>] e.g. [[:const, "ThingCreated"]]
      def events_produced_by(reactor, cmd_class)
        return [] unless @prism_available

        method_name = Sourced.message_method_name(Actor::PREFIX, cmd_class.name)
        extract_calls_from_handler(reactor, method_name, :event)
      end

      # Extract dispatch references from a reaction handler block.
      # @param reactor [Class]
      # @param evt_class [Class]
      # @return [Array<Array>] e.g. [[:const, "NotifyThing"]]
      def commands_dispatched_by(reactor, evt_class)
        return [] unless @prism_available

        method_name = Sourced.message_method_name(React::PREFIX, evt_class.name)
        extract_calls_from_handler(reactor, method_name, :dispatch)
      end

      private

      def check_prism
        defined?(::Prism)
      end

      def parse_file(path)
        @file_cache[path] ||= Prism.parse_file(path)
      end

      def extract_calls_from_handler(reactor, method_name, call_name)
        file, line = reactor.instance_method(method_name).source_location
        return [] unless file && line

        result = parse_file(file)
        block = find_block_at_line(result.value, line)
        return [] unless block

        collect_calls(block, call_name)
      rescue NameError
        []
      end

      # Find the block node at a specific line in the AST.
      def find_block_at_line(program, target_line)
        finder = BlockAtLineFinder.new(target_line)
        program.accept(finder)
        finder.found_block
      end

      # Collect all calls to the target method within a block, traversing all branches.
      def collect_calls(block_node, target_name)
        collector = CallCollector.new(target_name)
        block_node.accept(collector)
        collector.refs
      end

      if defined?(::Prism)
        # Prism visitor that finds the block at a specific source line.
        class BlockAtLineFinder < Prism::Visitor
          attr_reader :found_block

          def initialize(target_line)
            @target_line = target_line
            @found_block = nil
          end

          def visit_call_node(node)
            if node.location.start_line == @target_line && node.block
              @found_block = node.block
            end
            super
          end
        end

        # Prism visitor that collects references from call nodes.
        # Handles both direct calls (dispatch(Foo)) and chained calls (dispatch(Foo).at(...)).
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

          # Walk receiver chain to find the target call.
          # dispatch(Foo) => direct match
          # dispatch(Foo).to(id).at(time) => receiver chain
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
                  end
            @refs << ref if ref
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
