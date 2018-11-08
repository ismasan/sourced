require 'sourced/by_aggregate_id'

module Sourced
  class MemEventStore
    include ByAggregateId

    def initialize
      @events = []
      @mutex = Mutex.new
    end

    def append(evts)
      evts = Array(evts)
      mutex.synchronize {
        @events += evts
      }
      evts
    end

    private
    attr_reader :events, :mutex
  end
end
