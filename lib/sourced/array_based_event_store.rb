module Sourced
  # Mixin to provide EventStore interface
  # to any object that implements #events()[Event]
  # Ie. in-memory or simple test stores.
  module ArrayBasedEventStore
    def stream(entity_id: nil, from: nil, upto: nil)
      _events = events
      _events = events.find_all { |e| e.entity_id == entity_id } if entity_id
      if from
        idx = _events.index { |e| e.id == from}
        return [] unless idx
        _events = _events[idx..-1]
      end
      if upto
        idx = _events.index { |e| e.id == upto}
        return [] unless idx
        _events = _events[0..idx]
      end
      _events.to_enum
    end

    def by_entity_id(id, upto: nil, from: nil)
      stream(entity_id: id, upto: upto, from: from)
    end
  end
end
