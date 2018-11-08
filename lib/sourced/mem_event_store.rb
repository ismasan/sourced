module Sourced
  class MemEventStore
    def initialize
      @events = []
      @mutex = Mutex.new
    end

    def by_aggregate_id(id, upto: nil, from: nil)
      Enumerator.new do |yielder|
        events.each do |evt|
          next if from && evt.version <= from
          yielder.yield evt if evt.aggregate_id == id
          break if upto && upto == evt.version
        end
      end
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
