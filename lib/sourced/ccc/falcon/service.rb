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

          Async do |task|
            server.run
            CCC::Dispatcher.spawn_into(task)
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
