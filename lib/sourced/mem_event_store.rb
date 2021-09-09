# frozen_string_literal: true

require 'sourced/array_based_event_store'

module Sourced
  class MemEventStore
    include ArrayBasedEventStore

    def initialize
      @events = []
      @mutex = Mutex.new
    end

    def append_to_stream(stream_id, evts, expected_seq: nil)
      evts = [evts].flatten
      return evts unless evts.any?

      validate_stream_ids!(stream_id, evts)

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
