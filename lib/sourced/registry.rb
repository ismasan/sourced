# frozen_string_literal: true

require 'monitor'

module Sourced
  # A set of reactors, valid as a whole: what a {Router} routes to, and what
  # {Sourced.register} adds to and {Sourced.unregister} removes from, at any
  # time. The configured router and topology are built from {Sourced.registry}
  # (see {Config.build}), and a change on a built tree rebuilds them, so a
  # reactor registered or unregistered after boot is routed to, or not, from
  # then on.
  #
  #   registry = Sourced::Registry.new
  #   registry.register(Courses)       # and what Courses.to_sourced_reactors returns
  #   registry.unregister(Courses)
  #   registry.batch do                # one change, ex. for a code reloader
  #     registry.unregister(old)
  #     registry.register(new)
  #   end
  #
  # A reactor is checked on its own as it's registered: its definition, its
  # group_id's format and its partitioning. The set is checked as a whole when
  # the outermost batch ends: one reactor per group_id, an exclusive reactor the
  # only one handling its message types, and a machine's children registered
  # with it. Every change is a batch: a lone register or unregister is one of
  # its own. If the set is invalid, the registry goes back to what it was before
  # the batch and the error is raised. Otherwise its listeners hear of the
  # change. So a set is only invalid inside a batch, ex. while a reloader has
  # registered the new class but not yet unregistered the old one, with the
  # same group_id.
  #
  # Reactors are kept in registration order, once each, and identified by
  # identity: a reloaded class is a different reactor from the class it replaces.
  class Registry
    include Enumerable

    # Raised for a set with a reactor whose +to_sourced_reactors+ names reactors
    # that aren't in it, ex. a machine's child unregistered without the machine
    class MissingReactorsError < Sourced::Error
      def initialize(reactor, missing)
        super("#{reactor} needs #{missing.join(', ')} registered with it. " \
              "Unregister #{reactor} to unregister them")
      end
    end

    # @param reactors [Array<Class, #to_sourced_reactors>] registered in one batch
    # @raise what {#register} raises
    def initialize(reactors = [])
      @reactors = []
      @listeners = []
      @monitor = Monitor.new
      @batch_depth = 0
      @before = nil # the reactors when the outermost batch began
      batch { reactors.each { |r| register(r) } } if reactors.any?
    end

    # Register +reactor+, and what its +to_sourced_reactors+ returns, recursively
    # (a machine returns itself and the children it starts; anything else, ex. a
    # module, can return a group of reactors). A reactor already registered is
    # skipped. Each one gets the optional protocol methods it doesn't define
    # (see {ReactorDefaults}), and is checked on its own.
    # @param reactor [Class, #to_sourced_reactors] a reactor (see {Config::ReactorInterface})
    # @return [Class] the first reactor registered: +reactor+ itself, unless it's a group
    # @raise [ArgumentError] if +to_sourced_reactors+ returns nothing, if it isn't a
    #   reactor, for a group_id that isn't lowercase and dot-separated, or for a
    #   reactor that isn't exclusive and doesn't declare +partition_by+
    # @raise [DefinitionError] for a reactor whose definition is invalid, ex. a
    #   machine's, or whose history keys don't match its partition's
    # @raise when the batch ends, what the set's check raises (see {Registry}), ex.
    #   ArgumentError for a different reactor with the same group_id
    def register(reactor)
      reactors = ReactorDefaults.expand(reactor)
      raise ArgumentError, "#{reactor}.to_sourced_reactors returned no reactors" if reactors.empty?

      reactors.each { |r| check_reactor!(r) }
      change do
        reactors.each { |r| @reactors << r unless includes?(@reactors, r) }
      end
      reactors.first
    end

    # Unregister +reactor+, and what its +to_sourced_reactors+ returns except what
    # a reactor still registered returns too (a child another machine starts).
    # Reactors that aren't registered are skipped.
    # @param reactor [Class, #to_sourced_reactors]
    # @return [Boolean] whether anything was unregistered
    # @raise [MissingReactorsError] when the batch ends, for a reactor that one
    #   still registered needs, ex. a machine's child unregistered without the machine
    def unregister(reactor)
      going = ReactorDefaults.expand(reactor)
      removed = false
      change do
        staying = @reactors.reject { |r| includes?(going, r) }
        needed = staying.flat_map { |r| ReactorDefaults.expand(r) }
        kept = @reactors.select do |r|
          includes?(staying, r) || (includes?(needed, r) && !r.equal?(reactor))
        end
        removed = kept.size < @reactors.size
        @reactors = kept
      end
      removed
    end

    # @param reactor [Class]
    # @return [Boolean]
    def registered?(reactor) = @monitor.synchronize { includes?(@reactors, reactor) }

    # @return [Array<Class>] the registered reactors, in registration order
    def reactors = @monitor.synchronize { @reactors.dup.freeze }

    def each(&) = reactors.each(&)

    # Make the changes in the block one change, checked and heard of once, when the
    # outermost batch ends: if the block raises, or the new set is invalid, the
    # registry goes back to what it was before the batch, and the error is raised.
    # Batches hold the registry's lock, so other threads wait for them to end.
    # @return the block's value
    def batch
      changed = false
      result = @monitor.synchronize do
        outermost = @batch_depth.zero?
        @before = @reactors.dup if outermost
        @batch_depth += 1
        begin
          value = yield
          if outermost
            changed = @reactors != @before
            check_set!(@reactors) if changed
          end
          value
        rescue Exception # rubocop:disable Lint/RescueException -- put it back, whatever was raised. Always re-raised
          @reactors = @before if outermost
          changed = false
          raise
        ensure
          @batch_depth -= 1
          @before = nil if outermost
        end
      end
      notify_listeners if changed
      result
    end

    # Call +block+ with this registry after each change that changed something:
    # each register or unregister, or each batch, once. Listeners run outside the
    # registry's lock, so they can read it, and in the order they were added. A
    # listener that raises stops the ones after it, and the error propagates to the
    # call that made the change, which is kept.
    # @return [self]
    def on_change(&block)
      @monitor.synchronize { @listeners << block }
      self
    end

    private

    def change(&) = batch(&)

    def includes?(list, reactor) = list.any? { |r| r.equal?(reactor) }

    def notify_listeners
      @monitor.synchronize { @listeners.dup }.each { |listener| listener.call(self) }
    end

    # What holds of a reactor on its own, whatever else is registered
    def check_reactor!(reactor)
      ReactorDefaults.apply(reactor)
      # Reactors with a definition to check (ex. Sourced::Machine) fail at registration, not on first use.
      reactor.validate! if reactor.respond_to?(:validate!)
      unless Types::GroupIdFormat === reactor.group_id.to_s
        raise ArgumentError, "Cannot register #{reactor.group_label || reactor.inspect}: group_id #{reactor.group_id.inspect} " \
                             "isn't lowercase and dot-separated, like 'shop.orders' (see Sourced::Types::GroupId)"
      end

      # id-partitioning (declared as `partition_by :__id`, or implied by omitting
      # partition_by) indexes messages by Message#id (key name "__id", chosen to
      # not collide with a payload attribute) and must be sole-owned, so it is only
      # allowed for exclusive reactors. This keeps id-indexing safe: a type is only
      # id-indexed when its lone exclusive owner is id-partitioned.
      if reactor.claim_selector.id_partitioned? && !reactor.exclusive?
        raise ArgumentError,
          "#{reactor} must declare `partition_by`. (Only an `exclusive` reactor may " \
          "be id-partitioned — one partition per message — whether by omitting " \
          "partition_by or declaring `partition_by :__id`.)"
      end

      PartitionSchema.check_history_keys!(reactor)
    end

    # What holds of the reactors together. Each is checked against the ones
    # registered before it, so an error names the later of two.
    def check_set!(reactors)
      reactors.each_with_index do |reactor, i|
        earlier = reactors.take(i)
        check_group_id!(reactor, earlier)
        check_exclusive_ownership!(reactor, earlier)
      end
      reactors.each do |reactor|
        missing = ReactorDefaults.expand(reactor).reject { |r| includes?(reactors, r) }
        raise MissingReactorsError.new(reactor, missing) if missing.any?
      end
    end

    def check_group_id!(reactor, earlier)
      return unless (existing = earlier.find { |r| r.group_id == reactor.group_id })

      raise ArgumentError, "Cannot register #{reactor}: #{existing} already uses group_id #{reactor.group_id.inspect}"
    end

    # An exclusive reactor must be the sole handler of its message types: a message
    # deleted once the exclusive reactor acks it must not be needed by any other reactor.
    def check_exclusive_ownership!(reactor, earlier)
      types = reactor.claim_selector.type_names
      earlier.each do |existing|
        next unless reactor.exclusive? || existing.exclusive?

        overlap = types & existing.claim_selector.type_names
        next if overlap.empty?

        offender = reactor.exclusive? ? reactor : existing
        raise ArgumentError, <<~MSG.strip
          Cannot register #{reactor}: exclusive reactor #{offender} requires sole ownership of its message types, but #{reactor} and #{existing} both handle: #{overlap.sort.join(', ')}
        MSG
      end
    end
  end
end
