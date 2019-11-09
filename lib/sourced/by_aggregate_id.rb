module Sourced
  module ByAggregateId
    def by_aggregate_id(id, upto: nil, from: nil)
      Enumerator.new do |yielder|
        events.each do |evt|
          next if from && evt.version <= from
          yielder.yield evt if evt.aggregate_id == id
          break if upto && upto == evt.id
        end
      end
    end
  end
end
