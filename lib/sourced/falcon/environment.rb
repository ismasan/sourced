# frozen_string_literal: true

module Sourced
  module Falcon
    # Environment mixin for configuring a combined Falcon web server + Sourced workers service.
    #
    # Include this module in a Falcon service definition to get Sourced worker defaults
    # alongside the standard Falcon server environment.
    #
    # Housekeeping settings are read from Sourced.config by default (configured in your
    # boot/environment file). They can be overridden per-service in falcon.rb if needed.
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
      def sourced_worker_count = Sourced.config.worker_count

      # Number of housekeeper fibers to spawn per container process.
      # @return [Integer]
      def sourced_housekeeping_count = Sourced.config.housekeeping_count

      # Seconds between housekeeper scheduling cycles.
      # @return [Numeric]
      def sourced_housekeeping_interval = Sourced.config.housekeeping_interval

      # Seconds between worker heartbeats.
      # @return [Numeric]
      def sourced_housekeeping_heartbeat_interval = Sourced.config.housekeeping_heartbeat_interval

      # Seconds before a claim is considered stale and can be reaped.
      # @return [Numeric]
      def sourced_housekeeping_claim_ttl_seconds = Sourced.config.housekeeping_claim_ttl_seconds
    end
  end
end
