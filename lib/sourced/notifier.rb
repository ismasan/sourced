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
    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_INTERVAL = 1   # seconds, multiplied by attempt number
    MAX_RECONNECT_WAIT = 30  # cap

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
    # Reconnects on transient PG disconnects with linear backoff.
    # Re-raises after MAX_RECONNECT_ATTEMPTS so the error propagates to the supervisor.
    def run
      return unless active?

      @running = true
      retries = 0

      while @running
        begin
          @logger.info "Notifier: listening on #{CHANNEL}"

          db.listen(CHANNEL, timeout: 2, loop: true, after_listen: proc { retries = 0 }) do |_channel, _pid, payload|
            break unless @running

            types = payload.split(',')
            reactors = types.flat_map { |t| @type_to_reactors.fetch(t.strip, []) }.uniq
            reactors.each { |r| @work_queue.push(r) }
          end

          break # clean exit (@running set to false)

        rescue Sequel::DatabaseDisconnectError => e
          raise unless @running # don't retry during shutdown

          retries += 1
          if retries > MAX_RECONNECT_ATTEMPTS
            @logger.error "Notifier: reconnect failed after #{MAX_RECONNECT_ATTEMPTS} attempts"
            raise
          end

          wait = reconnect_wait(retries)
          @logger.warn "Notifier: connection lost (#{e.class}), reconnecting in #{wait}s (attempt #{retries}/#{MAX_RECONNECT_ATTEMPTS})"
          sleep wait
        end
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

    def reconnect_wait(retries)
      [RECONNECT_INTERVAL * retries, MAX_RECONNECT_WAIT].min
    end

    def db
      # Access the Sequel::Database through the backend's private accessor
      # SequelBackend exposes pubsub which has db, but we need the raw db
      @backend.send(:db)
    end
  end
end
