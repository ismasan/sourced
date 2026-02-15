# frozen_string_literal: true

module Sourced
  # Callable that maps message type strings to reactors and pushes them
  # onto a WorkQueue. Registered as the on_append callback on backend notifiers.
  class Notifier
    # @param work_queue [WorkQueue]
    # @param reactors [Array<Class>] reactor classes
    def initialize(work_queue:, reactors:)
      @work_queue = work_queue
      @type_to_reactors = build_type_lookup(reactors)
    end

    def call(types)
      reactors = types.flat_map { |t| @type_to_reactors.fetch(t.strip, []) }.uniq
      reactors.each { |r| @work_queue.push(r) }
    end

    private

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

  # Synchronous notifier for non-PG backends.
  # notify() invokes the callback directly. start/stop are no-ops.
  class InlineNotifier
    def on_append(callable)
      @on_append_callback = callable
    end

    def notify(types)
      @on_append_callback&.call(types.uniq)
    end

    def start = nil
    def stop = nil
  end
end
