# frozen_string_literal: true

module Sourced
  # Spawns a block into the context a component is started with: an
  # Async::Task (+async+), an executor task such as {ThreadExecutor} (+spawn+),
  # or, for any other context (ex. the Thread.current that Sourced::Component#start!
  # defaults to), a new thread.
  module Spawner
    module_function

    # @param context [#spawn, #async, Object]
    # @return [Object] what the context returned, or the Thread
    def into(context, &block)
      method = %i[spawn async].find { |m| context.respond_to?(m) }
      method ? context.public_send(method, &block) : Thread.new(&block)
    end
  end
end
