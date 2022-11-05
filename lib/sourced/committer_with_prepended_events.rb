# frozen_string_literal: true

module Sourced
  class CommitterWithPrependedEvents
    def initialize(committable, *events_to_prepend)
      @committable = committable
      @events_to_prepend = events_to_prepend
    end

    def commit(&_block)
      evts = to_a
      @committable.commit do |seq, _events, entity|
        yield seq, evts, entity
      end
      evts
    end

    def to_a
      @to_a ||= (
        evts = @committable.events
        seq = @committable.last_committed_seq
        (@events_to_prepend + evts).map do |evt|
          evt.copy(seq: seq += 1)
        end
      )
    end
  end
end
