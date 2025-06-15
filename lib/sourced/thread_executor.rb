# frozen_string_literal: true

require 'thread'

module Sourced
  class ThreadExecutor
    def initialize
      @stop_queue = Thread::Queue.new
      @threads = []
    end

    def to_s
      self.class.name
    end

    def start(&)
      yield self
      @threads.map(&:join)
    end

    def spawn(&work)
      @threads << Thread.new(&work)
    end
  end
end
