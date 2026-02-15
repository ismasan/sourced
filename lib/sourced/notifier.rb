# frozen_string_literal: true

module Sourced
  # PG LISTEN fiber that wakes workers when new messages arrive.
  # Listens on the `sourced_new_messages` channel. Payload is comma-separated
  # type strings (no JSON). Maintains an eager type→reactor lookup built from
  # each reactor's handled_messages.
  #
  # Only active for PostgreSQL — detected via db.adapter_scheme == :postgres.
  # For non-PG backends this is a no-op.
  class Notifier
    CHANNEL = 'sourced_new_messages'

    # @param work_queue [WorkQueue]
    # @param reactors [Array<Class>] reactor classes
    # @param backend [Object] the configured backend
    # @param logger [Object]
    def initialize(work_queue:, reactors:, backend:, logger: Sourced.config.logger)
      @work_queue = work_queue
      @backend = backend
      @logger = logger
      @running = false
      @type_to_reactors = build_type_lookup(reactors)
    end

    # Whether this notifier can actually listen (PG only).
    def active?
      pg_backend?
    end

    # Run the LISTEN loop. Blocks until stopped.
    # No-op for non-PG backends.
    def run
      unless active?
        @logger.info 'Notifier: not a PostgreSQL backend, skipping LISTEN'
        return
      end

      @running = true
      @logger.info "Notifier: listening on #{CHANNEL}"

      db.listen(CHANNEL, timeout: 2, loop: true) do |_channel, _pid, payload|
        break unless @running

        types = payload.split(',')
        reactors = types.flat_map { |t| @type_to_reactors.fetch(t.strip, []) }.uniq
        reactors.each { |r| @work_queue.push(r) }
      end

      @logger.info 'Notifier: stopped'
    end

    def stop
      @running = false
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

    def pg_backend?
      @backend.respond_to?(:pubsub) &&
        db.respond_to?(:adapter_scheme) &&
        db.adapter_scheme == :postgres
    end

    def db
      # Access the Sequel::Database through the backend's private accessor
      # SequelBackend exposes pubsub which has db, but we need the raw db
      @backend.send(:db)
    end
  end
end
