# frozen_string_literal: true

require 'digest'
require 'json'

module Sourced
  class Machine
    # How a snapshotting machine's state is stored: its runtime (positions,
    # region history, children) by path, and its context through the
    # snapshot's +type:+. Built once per machine, from its validated {Tree}.
    #
    # {#type} is what the store encodes and decodes the state with. Anything
    # that would make a stored state unusable fails that decode, before the
    # Router reads the history after it, so a stored state that decodes always
    # restores (see {#restore}):
    #
    # - +tree+ is a literal digest of the definition the runtime is written
    #   against: its states' paths, its regions, its children and their states;
    # - entering events are typed as one Hash schema per transition class,
    #   each with its +type+ a literal, so an event decodes to its own class,
    #   stored with its log position (nil for one not stored yet);
    # - the context is the snapshot's +type:+.
    # @api private
    class SnapshotCodec
      attr_reader :type, :digest

      # @param machine [Class<Machine>] validated
      # @param context_type [Plumb::Composable] the snapshot's +type:+
      def initialize(machine, context_type)
        @tree = machine.tree
        @regions = @tree.all_regions
        @region_index = @regions.each_with_index.to_h { |region, i| [region, i] }.compare_by_identity
        @classes = machine.__transition_classes.to_h { |klass| [klass.type, klass] }
        @children = @tree.children.to_h { |child| [child.group_id, child] }
        @child_states = @children.transform_values do |child|
          [*child.declared_states.values, child.start_state].compact.uniq.to_h { |handle| [handle.state_name.to_s, handle] }
        end
        @digest = Digest::SHA256.hexdigest(JSON.generate(fingerprint))
        @type = build_type(machine, context_type)
      end

      # The state to store, as {#type} takes it.
      #
      # @param runtime [Runtime]
      # @param context [Object] the context to store
      # @return [Hash]
      def dump(runtime, context)
        { tree: @digest, runtime: runtime.__dump(self), context: }
      end

      # A runtime at +state+'s positions.
      #
      # @param state [Hash] a decoded {#type}
      # @return [Runtime]
      def restore(state) = Runtime.new(@tree).__restore!(state[:runtime], self)

      # @api private
      def region_index(region) = @region_index.fetch(region)
      # @api private
      def region_at(index) = @regions.fetch(index)
      # @api private
      def node_at(path) = @tree.node_at(path)
      # @api private
      def event(attrs) = @classes.fetch(attrs[:type]).new(attrs)
      # @api private
      def child(group_id) = @children.fetch(group_id.to_s)
      # @api private
      def child_state(child, name) = @child_states.fetch(child.group_id).fetch(name)

      private

      # What the runtime's stored form depends on.
      def fingerprint
        {
          nodes: @tree.all_nodes.map(&:path),
          regions: @regions.map { |region| [region.path, region.name.to_s] },
          events: @classes.keys,
          children: @child_states.transform_values { |states| states.keys.sort }.sort.to_h
        }
      end

      def build_type(machine, context_type)
        t = Sourced::Types
        events = @classes.values.map { |klass| klass._schema + t::Hash[type: t::Value[klass.type]] }.reduce(:|)
        position = t::Hash[
          region: t::Integer[0...@regions.size],
          current: t::String,
          entered_by: t::Integer.nullable,
          visited: t::Array[t::String]
        ]
        history = t::Hash[region: t::Integer[0...@regions.size], current: t::String]
        # No children: nothing but an empty list decodes.
        children = @children.map { |group_id, child| t::Hash[child: t::Value[group_id], refs: child_refs_type(machine, child)] }.reduce(:|)
        children = children ? t::Array[children] : t::Tuple[]

        t::Hash[
          tree: t::Value[@digest],
          runtime: t::Hash[
            events: t::Array[t::Hash[message: events, position: t::Integer.nullable]],
            positions: t::Array[position],
            history: t::Array[history],
            children:
          ],
          context: context_type
        ]
      end

      # A child's refs: its keys are the child's partition keys the parent's
      # don't share, typed as the child declares them.
      def child_refs_type(machine, child)
        t = Sourced::Types
        own = machine.__child_own_keys(child)
        keys = child.partition_schema.to_h.to_h { |key, key_type| [key.to_sym, key_type] }.slice(*own)
        t::Array[t::Hash[
          keys: t::Hash[keys],
          state: t::String.options(@child_states.fetch(child.group_id).keys),
          final: t::Boolean,
          at: t::String.nullable,
          entry_id: t::String.nullable
        ]]
      end
    end
  end
end
