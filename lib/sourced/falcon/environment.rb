# frozen_string_literal: true

module Sourced
  module Falcon
    # Environment mixin for configuring a combined Falcon web server + workers service.
    #
    # Include this module in a Falcon service definition to get workers defaults
    # alongside the standard Falcon server environment. All settings are read from
    # {Sourced.config} — no per-service config methods needed.
    #
    # The Service automatically calls {Sourced.setup!} at the start of +run+ to
    # re-establish database connections after Falcon forks (SQLite connections
    # are not fork-safe). This replays the block passed to {Sourced.configure}.
    #
    # @example falcon.rb
    #   #!/usr/bin/env falcon-host
    #   require 'sourced/falcon'
    #   require_relative 'config/environment'
    #
    #   service "my-app" do
    #     include Sourced::Falcon::Environment
    #     include Falcon::Environment::Rackup
    #
    #     url "http://[::]:9292"
    #   end
    module Environment
      include ::Falcon::Environment::Server

      def service_class = Sourced::Falcon::Service
    end
  end
end
