# frozen_string_literal: true

require 'sourced/worker'
require 'sourced/house_keeper'

module Sourced
  module Falcon
    # A Falcon service that runs both the web server and Sourced background workers
    # as sibling fibers within the same Async reactor.
    #
    # This enables a single-process deployment model where Falcon's fiber-based
    # event loop hosts web requests and Sourced worker polling concurrently.
    # The Worker#poll and HouseKeeper#work methods use `sleep` calls which become
    # fiber yields in the Async context, allowing natural interleaving.
    #
    # Lifecycle is managed by Falcon's container — no need for a separate Supervisor
    # process or signal handling.
    class Service < ::Falcon::Service::Server
      def run(instance, evaluator)
        server = evaluator.make_server(@bound_endpoint)

        workers = evaluator.sourced_worker_count.times.map do |i|
          Sourced::Worker.new(name: "falcon-w-#{i}")
        end

        housekeepers = evaluator.housekeeping_count.times.map do |i|
          Sourced::HouseKeeper.new(
            backend: Sourced.config.backend,
            name: "falcon-hk-#{i}",
            interval: evaluator.housekeeping_interval,
            heartbeat_interval: evaluator.housekeeping_heartbeat_interval,
            claim_ttl_seconds: evaluator.housekeeping_claim_ttl_seconds,
            worker_ids_provider: -> { workers.map(&:name) }
          )
        end

        @sourced_workers = workers
        @sourced_housekeepers = housekeepers

        Async do |task|
          server.run
          housekeepers.each { |hk| task.async { hk.work } }
          workers.each { |w| task.async { w.poll } }
          task.children.each(&:wait)
        end

        server
      end

      def stop(...)
        @sourced_workers&.each(&:stop)
        @sourced_housekeepers&.each(&:stop)
        super
      end
    end
  end
end
