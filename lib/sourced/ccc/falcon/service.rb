# frozen_string_literal: true

require 'sourced/ccc/dispatcher'

module Sourced
  module CCC
    module Falcon
      # A Falcon service that runs both the web server and CCC background workers
      # as sibling fibers within the same Async reactor.
      #
      # Uses a CCC::Dispatcher for signal-driven worker dispatch. The Dispatcher
      # already embeds the StaleClaimReaper, so no separate HouseKeeper is needed
      # (unlike {Sourced::Falcon::Service}).
      #
      # All configuration is read from {CCC.config}.
      class Service < ::Falcon::Service::Server
        def run(instance, evaluator)
          CCC.setup!

          server = evaluator.make_server(@bound_endpoint)

          config = CCC.config
          @dispatcher = CCC::Dispatcher.new(
            router: CCC.router,
            worker_count: config.worker_count,
            batch_size: config.batch_size,
            max_drain_rounds: config.max_drain_rounds,
            catchup_interval: config.catchup_interval,
            housekeeping_interval: config.housekeeping_interval,
            claim_ttl_seconds: config.claim_ttl_seconds,
            logger: config.logger
          )

          Async do |task|
            server.run
            @dispatcher.spawn_into(task)
            task.children.each(&:wait)
          end

          server
        end

        def stop(...)
          @dispatcher&.stop
          super
        end
      end
    end
  end
end
