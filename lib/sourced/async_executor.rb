# frozen_string_literal: true

require 'async'

module Sourced
  # An executor
  # (for running a block of code concurrently)
  # that relies on Fibers via the Async gem.
  class AsyncExecutor
    class Task
      def initialize(task)
        @task = task
        freeze
      end

      def spawn(&block)
        @task.async(&block)
      end
    end

    def to_s
      self.class.name
    end

    def start(&)
      Sync do |task|
        yield Task.new(task)
      end
    end
  end
end
