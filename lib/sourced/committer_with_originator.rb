# frozen_string_literal: true

module Sourced
  class CommitterWithOriginator
    def initialize(originator, committable)
      @originator = originator
      @committable = committable
    end

    def commit(&_block)
      evts = to_a
      @committable.commit do |seq, _events|
        yield seq, evts
      end
    end

    def to_a
      @to_a ||= (
        [
          @originator,
          *@committable.events.map do |evt|
            evt.copy(originator_id: @originator.id)
          end
        ]
      )
    end
  end
end
