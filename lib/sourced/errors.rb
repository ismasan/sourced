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

  UnhandledCommandError = Class.new(EventError)

  AggregateError = Class.new(SourcedError)
  InvalidAggregateError = Class.new(AggregateError)

  ConcurrencyError = Class.new(SourcedError)
end
