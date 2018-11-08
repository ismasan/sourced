module Sourced
  class Dispatcher
    def initialize(repository:, store:, handler:)
      @repository, @store, @handler = repository, store, handler
    end

    def call(cmd)
      aggr, events = handler.call(cmd, repository: repository)
      store.append([cmd] + events)
      aggr
    end

    private
    attr_reader :repository, :store, :handler
  end
end
