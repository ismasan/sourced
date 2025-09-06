# frozen_string_literal: true

module Sourced
  class HouseKeeper
    attr_reader :name

    def initialize(
      logger: Sourced.config.logger,
      interval: 3,
      heartbeat_interval: 5,
      claim_ttl_seconds: 120,
      backend:,
      name:,
      worker_ids_provider: nil
    )
      @logger = logger
      @interval = interval
      @heartbeat_interval = heartbeat_interval
      @claim_ttl_seconds = claim_ttl_seconds
      @backend = backend
      @name = name
      @running = false
      @worker_ids_provider = worker_ids_provider || -> { [] }
    end

    def work
      @running = true

      # On start wait for a random period
      # in order to space out multiple house-keepers
      sleep rand(5)
      logger.info "HouseKeeper #{name}: starting"

      last_heartbeat = Time.at(0)
      while @running
        sleep @interval

        # 1) Schedule due messages
        schcount = backend.update_schedule!
        logger.info "HouseKeeper #{name}: appended #{schcount} scheduled messages" if schcount > 0

        now = Time.now

        # 2) Heartbeat alive workers (bulk upsert)
        if now - last_heartbeat >= @heartbeat_interval
          ids = Array(@worker_ids_provider.call).uniq
          hb = backend.worker_heartbeat(ids)
          logger.debug "HouseKeeper #{name}: heartbeated #{hb} workers" if hb > 0
          last_heartbeat = now
        end

        # 3) Reap stale claims
        released = backend.release_stale_claims(ttl_seconds: @claim_ttl_seconds)
        logger.info "HouseKeeper #{name}: released #{released} stale claims" if released && released > 0
      end

      logger.info "HouseKeeper #{name}: stopped"
    end

    def stop
      @running = false
    end

    private

    attr_reader :logger, :backend, :interval
  end
end
