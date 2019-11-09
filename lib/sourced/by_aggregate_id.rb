module Sourced
  module ByAggregateId
    def by_aggregate_id(id, upto: nil, from: nil)
      _events = events.find_all { |e| e.aggregate_id == id }

      if from
        idx = _events.index { |e| e.id == from}
        _events = _events[idx..-1] if idx
      end

      if upto
        idx = _events.index { |e| e.id == upto}
        _events = _events[0..idx] if idx
      end

      _events.to_enum
    end
  end
end
