# frozen_string_literal: true

require 'sourced/dispatcher'
require 'sourced/house_keeper'

module Sourced
  module Falcon
    # A Falcon service that runs both the web server and Sourced background workers
    # as sibling fibers within the same Async reactor.
    #
    # Uses a Dispatcher for signal-driven worker dispatch instead of blind polling.
    # Lifecycle is managed by Falcon's container — no need for a separate Supervisor
    # process or signal handling.
    class Service < ::Falcon::Service::Server
      def run(instance, evaluator)
        server = evaluator.make_server(@bound_endpoint)

        @dispatcher = Sourced::Dispatcher.new(
          router: Sourced::Router,
          worker_count: evaluator.sourced_worker_count,
          batch_size: evaluator.sourced_worker_batch_size,
          logger: Sourced.config.logger
        )

        housekeepers = evaluator.sourced_housekeeping_count.times.map do |i|
          Sourced::HouseKeeper.new(
            backend: Sourced.config.backend,
            name: "falcon-hk-#{i}",
            interval: evaluator.sourced_housekeeping_interval,
            heartbeat_interval: evaluator.sourced_housekeeping_heartbeat_interval,
            claim_ttl_seconds: evaluator.sourced_housekeeping_claim_ttl_seconds,
            worker_ids_provider: -> { @dispatcher.workers.map(&:name) }
          )
        end

        @sourced_housekeepers = housekeepers

        Async do |task|
          server.run
          housekeepers.each { |hk| task.async { hk.work } }
          @dispatcher.spawn_into(task)
          task.children.each(&:wait)
        end

        server
      end

      def stop(...)
        @dispatcher&.stop
        @sourced_housekeepers&.each(&:stop)
        super
      end
    end
  end
end
