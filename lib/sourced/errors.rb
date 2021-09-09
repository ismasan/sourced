# frozen_string_literal: true

module Sourced
  SourcedError = Class.new(StandardError)
  EventError = Class.new(SourcedError)
  UnknownEventError = Class.new(EventError)

  class InvalidEventError < EventError
    attr_reader :errors
    def initialize(topic, errors)
      @errors = errors
      msg = errors.map {|k, strings|
        "#{k} #{strings.join(', ')}"
      }.join('. ')
      super "Not a valid event '#{topic}': #{msg}"
    end
  end

  class ConcurrencyError < SourcedError
    def initialize(stream_id:, expected_seq:, current_seq:)
      super "attempting to append to stream '#{stream_id}' after seq #{expected_seq}, but last in store is #{current_seq}"
    end
  end

  class DifferentStreamIdError < SourcedError
    def initialize(expected_stream_id, unmatching_stream_id)
      super "trying to append event with :stream_id '#{unmatching_stream_id}' to stream #{expected_stream_id}"
    end
  end
end
