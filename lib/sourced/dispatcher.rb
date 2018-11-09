module Sourced
  class Dispatcher
    def initialize(repository: nil, store:, handler:, subscribers: Subscribers.new)
      @repository = repository || AggregateRepo.new(event_store: store)
      @store = store
      @handler = handler
      @subscribers = subscribers
    end

    def call(cmd)
      aggr, events = handler.call(cmd, repository: repository)
      subscribers.call(store.append(events))
      aggr
    end

    private
    attr_reader :repository, :store, :handler, :subscribers
  end
end
