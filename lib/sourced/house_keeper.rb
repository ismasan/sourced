# frozen_string_literal: true

module Sourced
  class HouseKeeper
    attr_reader :name

    def initialize(
      logger: Sourced.config.logger,
      interval: 3,
      backend:,
      name:
    )
      @logger = logger
      @interval = interval
      @backend = backend
      @name = name
      @running = false
    end

    def work
      @running = true

      # On start wait for a random period
      # in order to space out multiple house-keepers
      sleep rand(5)
      logger.info "HouseKeeper #{name}: starting"

      while @running
        sleep @interval
        schcount = backend.update_schedule!
        logger.info "HouseKeeper #{name}: appended #{schcount} scheduled messages" if schcount > 0
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
