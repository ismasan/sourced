# frozen_string_literal: true

module Sourced
  # Mixin to provide EventStore interface
  # to any object that implements #events()[Event]
  # Ie. in-memory or simple test stores.
  module ArrayBasedEventStore
    def read_stream(stream_id, upto_seq: nil)
      filter(stream_id: stream_id, upto_seq: upto_seq).sort_by(&:seq)
    end

    def transaction
      yield
    end

    private

    def filter(opts = {})
      opts = opts.dup
      upto_seq = opts.delete(:upto_seq)
      _events = events
      _events = _events.find_all do |e|
        opts.all?{ |k, v| e.to_h[k] == v }
      end
      if upto_seq
        ret = []
        _events.each do |evt|
          break if evt.seq > upto_seq
          ret << evt
        end
        _events = ret
      end
      _events.to_enum
    end

    def with_sequence_constraint(event, expected_seq, &_block)
      return yield unless expected_seq

      index = events.each.with_object(Hash.new(0)) do |evt, ret|
        ret[evt.stream_id] = evt.seq
      end

      current_seq = index[event.stream_id]
      if current_seq > expected_seq
        raise Sourced::ConcurrencyError.new(stream_id: event.stream_id, expected_seq: expected_seq, current_seq: current_seq)
      end

      yield
    end

    def validate_stream_ids!(stream_id, evts)
      unmatching_stream = evts.find { |h| h.stream_id != stream_id }
      raise DifferentStreamIdError.new(stream_id, unmatching_stream.stream_id) if unmatching_stream
    end
  end
end
