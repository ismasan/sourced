# frozen_string_literal: true

require 'thread'

module Sourced
  class ThreadExecutor
    def initialize
      @stop_queue = Thread::Queue.new
      @threads = []
    end

    def start(&)
      running = true

      yield self
      stops = 0
      while running && @stop_queue.pop
        stops += 1
        running = false if stops == @threads.size
      end
    end

    def spawn(&work)
      @threads << Thread.new do
        work.call
        @stop_queue << 1
      end
    end
  end
end
