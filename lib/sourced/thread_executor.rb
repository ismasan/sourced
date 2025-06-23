# frozen_string_literal: true

require 'thread'

module Sourced
  # An executor that runs blocks of code concurrently using Ruby threads.
  #
  # @example Basic usage
  #   executor = ThreadExecutor.new
  #   executor.start do |task|
  #     task.spawn { puts "First thread: #{Thread.current}" }
  #     task.spawn { puts "Second thread: #{Thread.current}" }
  #   end
  #
  class ThreadExecutor
    # Initialize a new thread executor
    # Sets up internal state for tracking spawned threads.
    def initialize
      @stop_queue = Thread::Queue.new
      @threads = []
    end

    # Return a string representation of this executor
    #
    # @return [String] The class name
    def to_s
      self.class.name
    end

    # Start the executor and yield itself for spawning concurrent work
    # This method will block until all spawned threads have completed.
    #
    # @yieldparam self [ThreadExecutor] The executor instance for spawning threads
    # @return [void] Blocks until all spawned threads complete
    def start(&)
      yield self
      @threads.map(&:join)
    end

    # Spawn a new thread to execute the given block concurrently
    #
    # @yieldparam work [Proc] The block to execute in a new thread
    # @return [Thread] The spawned thread
    def spawn(&work)
      @threads << Thread.new(&work)
    end
  end
end
