# frozen_string_literal: true

begin
  require 'prism'
rescue LoadError
  # Prism not available; SourceAnalyzer will return empty produces arrays.
end

module Sourced
  # Analyzes registered reactors (Deciders and Projectors) and builds a
  # flat array of node structs describing the message flow graph. Enables
  # visualization, introspection, and Event Modeling diagram generation.
  module Topology
    CommandNode = Struct.new(:type, :id, :name, :group_id, :produces, :schema, keyword_init: true)
    EventNode = Struct.new(:type, :id, :name, :group_id, :produces, :schema, keyword_init: true)
    AutomationNode = Struct.new(:type, :id, :name, :group_id, :consumes, :produces, keyword_init: true)
    ReadModelNode = Struct.new(:type, :id, :name, :group_id, :consumes, :produces, :schema, keyword_init: true)

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

        if reactor.respond_to?(:handled_commands)
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
              name: "reaction(#{group_id})",
              group_id: group_id,
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
            name: group_id,
            group_id: group_id,
            consumes: consumes,
            produces: projector_aut_ids,
            schema: {}
          )
        end
      end

      nodes + event_nodes.values
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

    private_class_method :resolve_refs, :resolve_ref, :resolve_symbol_ref,
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

        method_name = Sourced.message_method_name('sourced_decide', cmd_class.name)
        extract_calls_from_handler(reactor, method_name, :event)
      end

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
