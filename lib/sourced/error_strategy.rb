# frozen_string_literal: true

module Sourced
  class ErrorStrategy
    # @param exception [Exception]
    # @param message [Sourced::Message]
    # @param group [#retry, #stop]
    def call(exception, message, group)
      group.stop(exception:, message:)
    end
  end
end
