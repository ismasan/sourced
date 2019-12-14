require 'sourced/array_based_event_store'

module Sourced
  class MemEventStore
    include ArrayBasedEventStore

    def initialize
      @events = []
      @mutex = Mutex.new
    end

    def append(evts, expected_seq: nil)
      evts = Array(evts)
      mutex.synchronize {
        with_sequence_constraint(evts.last, expected_seq) do
          @events += evts
        end
      }
      evts
    end

    private
    attr_reader :events, :mutex
  end
end
