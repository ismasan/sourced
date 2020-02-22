# frozen_string_literal: true

module Sourced
  # Mixin to provide EventStore interface
  # to any object that implements #events()[Event]
  # Ie. in-memory or simple test stores.
  module ArrayBasedEventStore
    def filter(opts = {})
      opts = opts.dup
      after = opts.delete(:after)
      upto = opts.delete(:upto)
      _events = events
      _events = _events.find_all do |e|
        opts.all?{ |k, v| e.to_h[k] == v }
      end
      # _events = events.find_all { |e| e.entity_id == entity_id } if entity_id
      if after
        idx = _events.index { |e| e.id == after}
        return [] unless idx
        _events = _events[idx+1..-1]
      end
      if upto
        idx = _events.index { |e| e.id == upto}
        return [] unless idx
        _events = _events[0..idx]
      end
      _events.to_enum
    end

    def by_entity_id(id, upto: nil, after: nil)
      filter(entity_id: id, upto: upto, after: after)
    end

    def transaction
      yield
    end

    private

    def with_sequence_constraint(event, expected_seq, &_block)
      return yield unless expected_seq

      index = events.each.with_object(Hash.new(0)) do |evt, ret|
        ret[evt.entity_id] = evt.seq
      end

      current_seq = index[event.entity_id]
      if current_seq > expected_seq
        raise Sourced::ConcurrencyError, "attempting to append entity #{event.entity_id} after seq #{expected_seq}, but last in store is #{current_seq}"
      end

      yield
    end
  end
end
