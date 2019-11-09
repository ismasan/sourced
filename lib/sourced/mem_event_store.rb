require 'sourced/array_based_event_store'

module Sourced
  class MemEventStore
    include ArrayBasedEventStore

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
