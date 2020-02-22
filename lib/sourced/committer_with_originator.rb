# frozen_string_literal: true

module Sourced
  class CommitterWithOriginator
    def initialize(originator, committable)
      @originator = originator
      @committable = committable
    end

    def commit(&_block)
      evts = to_a
      @committable.commit do |seq, _events, entity|
        yield seq, evts, entity
      end
    end

    def to_a
      @to_a ||= (
        evts = @committable.events
        [
          @originator.copy(seq: evts.first.seq),
          *evts.map do |evt|
            evt.copy(originator_id: @originator.id, seq: evt.seq + 1)
          end
        ]
      )
    end
  end
end
