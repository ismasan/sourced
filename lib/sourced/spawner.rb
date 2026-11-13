# frozen_string_literal: true

require 'async'

module Sourced
  # Spawns a block as a fiber, into the context a component is started with:
  # an executor task ({AsyncExecutor}'s, +spawn+) or an Async::Task (+async+).
  # Any other context (ex. the Thread.current that Sourced::Component#start!
  # defaults to) spawns into the current Async task, so starting inside
  # +Sync { }+ / +Async { }+ needs no task.
  module Spawner
    # Raised when there's neither a task to spawn into nor a current Async task.
    class NoTaskError < Sourced::Error
      def initialize(context)
        super("can't spawn into #{context.inspect}: start Sourced inside an Async task, ex. " \
              '`Sync { |task| Sourced.start!(task) }`, or with Sourced::Supervisor')
      end
    end

    module_function

    # @param context [#spawn, #async, Object]
    # @return [Object] what the task returned
    # @raise [NoTaskError] outside an Async task, for a context that can't spawn
    def into(context, &block)
      method = %i[spawn async].find { |m| context.respond_to?(m) }
      return context.public_send(method, &block) if method

      task = Async::Task.current?
      raise NoTaskError, context unless task

      task.async(&block)
    end
  end
end
