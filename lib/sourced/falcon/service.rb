# frozen_string_literal: true

require 'sourced/dispatcher'

module Sourced
  module Falcon
    # A Falcon service that runs both the web server and Sourced background workers
    # as sibling fibers within the same Async reactor.
    #
    # Uses a Sourced::Dispatcher for signal-driven worker dispatch. The Dispatcher
    # already embeds the StaleClaimReaper, so no separate HouseKeeper is needed
    # (unlike {Sourced::Falcon::Service}).
    #
    # All configuration is read from {Sourced.config}.
    class Service < ::Falcon::Service::Server
      def run(instance, evaluator)
        Sourced.setup!

        server = evaluator.make_server(@bound_endpoint)

        Async do |task|
          server.run
          Sourced::Dispatcher.start(task)
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
