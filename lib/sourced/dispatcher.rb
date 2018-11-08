module Sourced
  class Dispatcher
    def initialize(repository:, store:, handler:, subscribers: Subscribers.new)
      @repository = repository
      @store = store
      @handler = handler
      @subscribers = subscribers
    end

    def call(cmd)
      aggr, events = handler.call(cmd, repository: repository)
      subscribers.call(store.append([cmd] + events))
      aggr
    end

    private
    attr_reader :repository, :store, :handler, :subscribers
  end
end
