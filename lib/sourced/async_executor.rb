# frozen_string_literal: true

require 'async'

module Sourced
  # An executor that runs blocks of code concurrently using fibers
  # via the Async gem. This is the default executor for Sourced.
  #
  # @example Basic usage
  #   executor = AsyncExecutor.new
  #   executor.start do |task|
  #     task.spawn { puts "First task" }
  #     task.spawn { puts "Second task" }
  #   end
  #
  # @see https://github.com/socketry/async Async gem documentation
  class AsyncExecutor
    # A wrapper around Async::Task that provides the spawn interface
    # for creating concurrent fiber-based tasks.
    class Task
      # Initialize a new task wrapper
      #
      # @param task [Async::Task] The underlying async task
      def initialize(task)
        @task = task
        @tasks = []
        freeze
      end

      # Spawn a new concurrent fiber within this task's context
      #
      # @yieldparam block [Proc] The block to execute concurrently
      # @return [Async::Task] The spawned async task
      def spawn(&block)
        @task.async(&block).tap do |t|
          @tasks << t
        end
      end

      def wait = @tasks.each(&:wait)
    end

    # Return a string representation of this executor
    #
    # @return [String] The class name
    def to_s
      self.class.name
    end

    # Start the executor and yield a task interface for spawning concurrent work
    #
    # @yieldparam task [Task] Interface for spawning concurrent tasks
    # @return [void] Blocks until all spawned tasks complete
    def start(&)
      Sync do |task|
        Task.new(task).tap do |t|
          yield t
        end
      end.wait
    end
  end
end
