# frozen_string_literal: true

require 'async'
require 'async/queue'
require 'async/barrier'

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
      # @option wait [Boolean] Whether to wait for tasks
      def initialize(wait: true)
        @wait = wait
        @blocks = []
        @barrier = nil
        # freeze
      end

      # Spawn a new concurrent fiber within this task's context
      #
      # @yieldparam block [Proc] The block to execute concurrently
      # @return [Async::Task] The spawned async task
      def spawn(&block)
        @blocks << block
        self
      end

      def start
        @barrier = Async::Barrier.new
        Async(transient: !@wait) do |t|
          @blocks.each do |bl|
            @barrier.async(&bl)
          end
        end
      end

      def wait = @barrier&.wait
    end

    def self.start(&)
      new.start(&)
    end

    # Return a string representation of this executor
    #
    # @return [String] The class name
    def to_s
      self.class.name
    end

    def new_queue
      Async::Queue.new
    end

    # Start the executor and yield a task interface for spawning concurrent work
    #
    # @yieldparam task [Task] Interface for spawning concurrent tasks
    # @return [void] Blocks until all spawned tasks complete
    def start(wait: true, &)
      task = Task.new(wait:)
      yield task if block_given?
      task.start
      task.wait if wait
      task
    end
  end
end
