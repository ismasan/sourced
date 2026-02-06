# frozen_string_literal: true

module Sourced
  module Falcon
    # Environment mixin for configuring a combined Falcon web server + Sourced workers service.
    #
    # Include this module in a Falcon service definition to get Sourced worker defaults
    # alongside the standard Falcon server environment.
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
    #     sourced_worker_count 4
    #   end
    module Environment
      include ::Falcon::Environment::Server

      def service_class
        Sourced::Falcon::Service
      end

      # Number of Sourced worker fibers to spawn per container process.
      # @return [Integer]
      def sourced_worker_count = 2

      # Number of housekeeper fibers to spawn per container process.
      # @return [Integer]
      def housekeeping_count = 1

      # Seconds between housekeeper scheduling cycles.
      # @return [Numeric]
      def housekeeping_interval = 3

      # Seconds between worker heartbeats.
      # @return [Numeric]
      def housekeeping_heartbeat_interval = 5

      # Seconds before a claim is considered stale and can be reaped.
      # @return [Numeric]
      def housekeeping_claim_ttl_seconds = 120
    end
  end
end
