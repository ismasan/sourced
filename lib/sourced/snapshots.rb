# frozen_string_literal: true

module Sourced
  # A partition's stored snapshot, as the Router hands it to a snapshotting
  # reactor's +handle_claim(claim, history:, snapshot:)+: the reactor's state
  # up to {#position}, the last history message folded into it. The history
  # the reactor is given starts after it.
  class Snapshot
    NONE = Object.new.freeze

    # @return [Integer]
    attr_reader :position

    # @param position [Integer]
    # @param first [Object] an already decoded state, handed out by the first {#state}
    # @yield decodes the stored state
    def initialize(position:, first: NONE, &decode)
      @position = position
      @first = first
      @decode = decode
    end

    # The decoded state. Each call returns its own copy (decoded again), so
    # state a reactor evolves from one call doesn't leak into another's.
    #
    # @return [Object]
    def state
      return @decode.call if NONE.equal?(@first)

      first = @first
      @first = NONE
      first
    end
  end

  # Partition snapshots for reactors that evolve their state from history:
  # the Router stores the reactor's state for a partition every so often, and
  # later claims read only the history after it. A snapshot is a cache, never
  # the truth: without one, a claim reads the partition's whole history.
  #
  #   class ScanQueue < Sourced::Decider
  #     partition_by :queue_name
  #     snapshot every: 100, version: 1
  #   end
  #
  # +every+: write a snapshot once a claim's history, after the stored
  # snapshot, has this many messages. +version+: bump it whenever +evolve+ or
  # the state's shape changes; a snapshot written at another version is
  # ignored. Forgetting to bump it gives silently wrong state. +type+: the
  # state's type, which the store encodes and decodes it with (through
  # +Plumb::Codec::JSON+); {Types::JSONData} by default (JSON-native values,
  # Hash keys decoded as Symbols). A stored state that no longer decodes
  # against it is ignored.
  #
  # Hooks, both optional: +#to_snapshot+ (the state to store; the context by
  # default) and +.from_snapshot(state)+ (the context to start from; the
  # decoded state by default), for mapping what +type+ can't express.
  #
  # == Reactor protocol
  #
  # The Router sees a snapshotting reactor through:
  #
  # - +snapshot_config+: a {Config}, or nil ({ReactorDefaults} gives other
  #   reactors nil). Read once, when the reactor is added.
  # - +handle_claim(claim, history:, snapshot:)+: +snapshot+ is a {Snapshot}
  #   or nil, and +history+ starts after its position.
  # - a +{ type: :snapshot, state:, position: }+ signal ({Actions::Snapshot})
  #   in its returned actions: the Router stores +state+ in the claim's ack
  #   transaction, only if the ack is for the current claim. +position+ must be
  #   below the claim's first message (see {ClassMethods#snapshot_signal}).
  #
  # The mixin implements that for reactors whose class answers
  # +new(partition_values)+ and whose instances answer +#evolve(messages)+ and
  # +#context+ (ex. through {Evolve}). Such a reactor's +handle_claim+ starts
  # its instance from {ClassMethods#snapshot_instance}, and adds
  # {ClassMethods#snapshot_signal} to its actions with
  # {ClassMethods#with_snapshot_signal}. See {Decider.handle_claim} and
  # {Machine.handle_claim}. An instance that holds more than its context
  # overrides {#snapshot_state} and {#restore_snapshot_state} (as {Machine}
  # does for its runtime), leaving the hooks above to its users.
  module Snapshots
    # How a reactor snapshots (see {ClassMethods#snapshot}).
    class Config < Types::Data
      # History messages after the stored snapshot that trigger a new one.
      attribute :every, Types::Integer[1..]
      # The state's version.
      attribute :version, Types::Integer
      # The state's type, a Plumb type.
      attribute :type, Types::Any[Plumb::Composable]
    end

    def self.included(base)
      super
      # Prepended so +validate!+ runs over Consumer's, as Evolve's do.
      base.singleton_class.prepend ClassMethods
    end

    # The context to store. Default: the context itself. Called on an
    # instance only used to take the snapshot, so it may return the context.
    #
    # @return [Object] a value of the snapshot's +type+
    def to_snapshot = context

    # Start from +context+ instead of the context block.
    #
    # @param context [Object]
    # @return [void]
    def restore_snapshot(context)
      @projection = self.class.projector.new(context)
    end

    # The whole state to store: what {ClassMethods#snapshot_signal} stores.
    # Default: {#to_snapshot}. A reactor whose instances hold more than their
    # context (ex. {Machine}'s runtime) overrides this and
    # {#restore_snapshot_state}, leaving {#to_snapshot} / {ClassMethods#from_snapshot}
    # to its users, for the context.
    #
    # @return [Object] a value of the snapshot's +type+
    def snapshot_state = to_snapshot

    # Start from a stored {#snapshot_state}, decoded.
    #
    # @param state [Object]
    # @return [void]
    def restore_snapshot_state(state)
      restore_snapshot(self.class.from_snapshot(state))
    end

    module ClassMethods
      # Declare that this reactor snapshots its partitions' state.
      #
      # @param every [Integer]
      # @param version [Integer]
      # @param type [Plumb::Composable]
      # @return [void]
      # @raise [Plumb::ParseError] for invalid arguments
      def snapshot(every:, version:, type: Types::JSONData)
        @snapshot_config = Config.parse(every:, version:, type:)
      end

      # @return [Config, nil] see {Snapshots}
      def snapshot_config = declared_snapshot_config

      # The {Config} +snapshot+ declared, here or on a superclass.
      # @api private
      # @return [Config, nil]
      def declared_snapshot_config
        return @snapshot_config if defined?(@snapshot_config)

        superclass.respond_to?(:declared_snapshot_config) ? superclass.declared_snapshot_config : nil
      end

      # The context an instance starts from, given a snapshot's decoded state.
      # Override to map what the snapshot's +type+ can't express.
      #
      # @param state [Object]
      # @return [Object]
      def from_snapshot(state) = state

      # An instance for the partition, started from +snapshot+'s state when given.
      #
      # @param partition_values [Hash{Symbol => String}]
      # @param snapshot [Snapshot, nil]
      # @return [Object] an instance
      def snapshot_instance(partition_values, snapshot)
        instance = new(partition_values)
        instance.restore_snapshot_state(snapshot.state) if snapshot
        instance
      end

      # The snapshot to store for a claim, or nil when it isn't due.
      #
      # It covers +history+ before +before+ (the claim's first message): a
      # claimed message can be in the history too (an event the reactor reacts
      # to), and is evolved by the claim, not read as history, so a snapshot
      # past it would skip it on every later claim. Its state is taken from an
      # instance of its own (from +snapshot+ again), never the one the claim
      # decides with: what the claim decides hasn't committed.
      #
      # Due when that part of the history has +every+ messages or more (with no
      # snapshot, it's the whole history).
      #
      # @param partition_values [Hash{Symbol => String}]
      # @param snapshot [Snapshot, nil] the snapshot +history+ starts after
      # @param history [#to_a] the history read for the claim, excluding nothing
      #   or its claimed messages
      # @param before [#position] the claim's first message
      # @return [Actions::Snapshot, nil]
      def snapshot_signal(partition_values, snapshot, history, before:)
        config = snapshot_config
        return unless config

        prior = history.to_a.select { |m| m.position < before.position }
        return if prior.size < config.every

        instance = snapshot_instance(partition_values, snapshot)
        instance.evolve(prior)
        Actions::Snapshot.new(state: instance.snapshot_state, position: prior.last.position)
      end

      # +pairs+ with +signal+ added to the first pair's signals, so it's stored
      # when any of the claim is acked.
      #
      # @param pairs [Array<Array(Object, Sourced::Message)>]
      # @param signal [Actions::Snapshot, nil]
      # @return [Array<Array(Object, Sourced::Message)>]
      def with_snapshot(pairs, signal)
        return pairs unless signal && pairs.is_a?(Array) && pairs.any?

        (signals, message), *rest = pairs
        signals = signals.is_a?(Array) ? signals : [signals]
        [[[signal, *signals], message], *rest]
      end

      # Run +block+ (a claim's handling, returning its pairs), adding +signal+
      # to its pairs, or to the action pairs of the {PartialBatchError} it
      # raises, so it's stored when any of the claim is acked.
      #
      # @param signal [Actions::Snapshot, nil]
      # @yieldreturn [Array<Array(Object, Sourced::Message)>]
      # @return [Array<Array(Object, Sourced::Message)>]
      def with_snapshot_signal(signal)
        with_snapshot(yield, signal)
      rescue PartialBatchError => e
        raise e unless signal

        raise PartialBatchError.new(with_snapshot(e.action_pairs, signal), e.failed_message, e.cause || e), cause: e.cause
      end

      # @raise [DefinitionError] for a snapshotting reactor that can't rebuild
      #   its state from history, or reads it through a {Selector}
      def validate!
        valid = super
        return valid unless declared_snapshot_config

        if exclusive?
          raise DefinitionError, "#{self} is exclusive, so its messages may be deleted on ack: " \
                                 "its state can't be rebuilt from history, so it can't snapshot it"
        end
        if partition_keys.empty?
          raise DefinitionError, "#{self} is id-partitioned (one partition per message): " \
                                 'it has no partition state to snapshot'
        end
        if source.is_a?(Selector)
          raise DefinitionError, "#{self} evolves #{source.inspect}, whose context is the messages it reads: " \
                                 'snapshots need an inline projector'
        end

        valid
      end
    end
  end
end
