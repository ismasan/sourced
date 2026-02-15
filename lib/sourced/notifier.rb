# frozen_string_literal: true

module Sourced
  # Maps message type strings to interested reactor classes and pushes them
  # onto a {WorkQueue}. Implements +#call+ so it can be registered as the
  # +on_append+ callback on any backend notifier ({InlineNotifier} or
  # {Backends::SequelBackend::PGNotifier}).
  #
  # Builds an eager type-to-reactor lookup at initialization from each
  # reactor's +handled_messages+. Unknown types are silently ignored.
  #
  # @example Registering as a callback
  #   notifier = Notifier.new(work_queue: queue, reactors: [OrderReactor, ShipReactor])
  #   backend.notifier.on_append(notifier)
  #
  #   # When new messages are appended:
  #   notifier.call(['orders.created'])
  #   # => pushes OrderReactor (and any other reactor handling that type) onto the queue
  class Notifier
    # @param work_queue [WorkQueue] queue to push signaled reactors onto
    # @param reactors [Array<Class>] reactor classes whose +handled_messages+
    #   define the type-to-reactor mapping
    def initialize(work_queue:, reactors:)
      @work_queue = work_queue
      @type_to_reactors = build_type_lookup(reactors)
    end

    # Look up reactors interested in the given message types and push each
    # onto the work queue. Deduplicates reactors across types.
    #
    # @param types [Array<String>] message type strings (e.g. +'orders.created'+)
    # @return [void]
    def call(types)
      reactors = types.flat_map { |t| @type_to_reactors.fetch(t.strip, []) }.uniq
      reactors.each { |r| @work_queue.push(r) }
    end

    private

    # @return [Hash{String => Array<Class>}] mapping from type string to reactor classes
    def build_type_lookup(reactors)
      lookup = Hash.new { |h, k| h[k] = [] }
      reactors.each do |reactor|
        reactor.handled_messages.map(&:type).uniq.each do |type|
          lookup[type] << reactor
        end
      end
      lookup
    end
  end

  # Synchronous backend notifier for non-PG backends (TestBackend, SQLite).
  # Invokes the +on_append+ callback directly within +notify+.
  # +start+ and +stop+ are no-ops since there is no background listener.
  #
  # Implements the same interface as {Backends::SequelBackend::PGNotifier}:
  # +on_append+, +notify+, +start+, +stop+.
  #
  # @example
  #   notifier = InlineNotifier.new
  #   notifier.on_append(->(types) { puts types })
  #   notifier.notify(['orders.created', 'orders.created'])
  #   # prints: ["orders.created"]
  class InlineNotifier
    # Register a callback to be invoked when new messages are appended.
    #
    # @param callable [#call] receives an Array of unique type strings
    # @return [void]
    def on_append(callable)
      @on_append_callback = callable
    end

    # Invoke the +on_append+ callback synchronously with deduplicated types.
    #
    # @param types [Array<String>] message type strings
    # @return [void]
    def notify(types)
      @on_append_callback&.call(types.uniq)
    end

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def start = nil

    # No-op. Provided for interface compatibility with {Backends::SequelBackend::PGNotifier}.
    # @return [nil]
    def stop = nil
  end
end
