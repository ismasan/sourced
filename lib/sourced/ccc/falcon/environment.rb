# frozen_string_literal: true

module Sourced
  module CCC
    module Falcon
      # Environment mixin for configuring a combined Falcon web server + CCC workers service.
      #
      # Include this module in a Falcon service definition to get CCC worker defaults
      # alongside the standard Falcon server environment. All settings are read from
      # {CCC.config} — no per-service config methods needed.
      #
      # The Service automatically calls {CCC.setup!} at the start of +run+ to
      # re-establish database connections after Falcon forks (SQLite connections
      # are not fork-safe). This replays the block passed to {CCC.configure}.
      #
      # @example falcon.rb
      #   #!/usr/bin/env falcon-host
      #   require 'sourced/ccc/falcon'
      #   require_relative 'config/environment'
      #
      #   service "my-app" do
      #     include Sourced::CCC::Falcon::Environment
      #     include Falcon::Environment::Rackup
      #
      #     url "http://[::]:9292"
      #   end
      module Environment
        include ::Falcon::Environment::Server

        def service_class = CCC::Falcon::Service
      end
    end
  end
end
